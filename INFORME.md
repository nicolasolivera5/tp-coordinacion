Redactar un breve informe en el archivo `INFORME.md` explicando el modo en que se coordinan las instancias de Sum y Aggregation, así como el modo en el que el sistema escala respecto a los clientes, grándes volúmens de datos y la cantidad de controles.

# Informe de Coordinación de Sistemas Distribuidos
---

## 1. Aislamiento Multi-Cliente
Para resolver consultas de múltiples clientes de forma concurrente sin que sus flujos de datos interfieran entre sí:
- **Identificador Único (`client_id`)**: Al conectarse un cliente por TCP, el `Gateway` instancia un `MessageHandler` que le asigna un identificador único (UUIDv4).
- **Protocolo**: Todo mensaje que viaja por los canales del middleware incluye `client_id` como primer campo (`[client_id, fruit, amount]` para datos y `[client_id]` para fin de datos o `EOF`).
- **Estado por Cliente**: Tanto en las instancias de `Sum` como en las de `Aggregation`, las estructuras de datos en memoria segregan el estado utilizando `client_id` como clave (e.g. `amount_by_fruit_and_client[client_id]` y `fruit_top_for_client[client_id]`). De esta forma, el procesamiento y cálculo de tops para distintos clientes es completamente independiente.

---

## 2. Coordinación entre Réplicas de `Sum`
En la etapa de `Sum`, todas las réplicas compiten como consumidores concurrentes sobre la misma cola compartida de entrada (`input_queue`). Esta arquitectura genera dos desafíos fundamentales:
1. **Notificación de fin de datos única**: Los mensajes de datos de un cliente se distribuyen entre las distintas réplicas de `Sum`, pero el mensaje de `EOF` es tomado por solo una de ellas desde la cola `input_queue`.
2. **Condiciones de carrera y desincronización**: Las réplicas que no recibieron el `EOF` directamente deben enterarse de la finalización del cliente para vaciar sus acumuladores hacia `Aggregation`, pero sin emitir sus resultados antes de que se hayan terminado de procesar los mensajes de datos que aún se encuentran en tránsito o en buffers locales.

---

### 2.1 Evolución de las Soluciones Analizadas

Durante el diseño e iteración del sistema se evaluaron distintas alternativas para resolver la coordinación entre las réplicas de `Sum`:

#### Versión 1: Broadcast simple con `RLock` y `prefetch_count=1`
- **Idea**: La réplica que extraía el `EOF` de `input_queue` emitía un broadcast por un topic exchange (`SUM_CONTROL_EXCHANGE`) a las demás réplicas. El acceso a las estructuras en memoria se protegía con un `threading.RLock()`.
- **Falencia identicada**: Aunque `prefetch_count=1` asegura que un consumidor no acumule más de un mensaje sin confirmar en su buffer local de RabbitMQ, la entrega de mensajes a través del broker hacia múltiples consumidores corre sobre conexiones TCP independientes. RabbitMQ no espera el ACK de un consumidor antes de despachar el siguiente mensaje a otro consumidor libre. Por ende, un mensaje de datos podía estar viajando por TCP hacia `Sum_1` mientras el broker ya le entregaba el `EOF` a `Sum_2`. Si `Sum_2` emitía el broadcast de inmediato, el aviso de fin podía ganarle al dato en vuelo de `Sum_1`, provocando que este vaciara sus resultados omitiendo dicho registro.

#### Versión 2: Token circulante por la misma cola (`input_queue`)
- **Idea**: En lugar de utilizar un exchange de control separado, el nodo que recibía el `EOF` reinyectaba un token con un contador decremental en la propia `input_queue`. Como los datos y el token viajaban por la misma cola FIFO, se garantizaba formalmente que el token siempre llegaba detrás de los datos.
- **Descarte**: Aunque formalmente correcto, este esquema presentaba serios problemas de escalabilidad y latencia. Si un nodo que ya había procesado el token volvía a extraerlo de la cola por round-robin, debía reencolarlo repetidamente hasta que todos los demás nodos lo hubiesen consumido. Esto generaba un efecto de *busy-polling* sobre la cola compartida y una sobrecarga innecesaria sobre el broker RabbitMQ.

#### Versión 3: Coordinación delegada a `Aggregation`
- **Idea**: Eliminar la coordinación interna entre réplicas de `Sum` y delegar la detección del fin de datos en las instancias de `Aggregation`.
- **Descarte**: `Aggregation` particiona las frutas por hash (`sha256(fruit) % AGGREGATION_AMOUNT`) y desconoce de antemano cuántos mensajes o qué frutas procesó cada réplica de `Sum`. Para que `Aggregation` pudiera detectar el fin global sin un broadcast previo de `Sum`, se requería que cada nodo de `Sum` conociera el total de mensajes de cada partición o que `Aggregation` implementara una compleja matriz de sincronización cruzada, acoplando innecesariamente responsabilidades de distintas etapas del pipeline.

---

### 2.2 Solución Final Implementada: Conteo Distribuido y Barrera Reactiva de 2 Fases

La solución definitiva adoptada desacopla el orden temporal de las conexiones TCP y garantiza convergencia mediante la **conservación de mensajes** y un esquema de dos fases (`PREPARE` / `READY` / `COMMIT`) con enrutamiento dirigido:

1. **Numeración y Total en Gateway (`MessageHandler`)**:
   - Cada cliente mantiene un contador `msg_count` que se incrementa con cada mensaje de datos emitido.
   - Al finalizar la ingesta, el Gateway emite el `EOF` conteniendo el identificador del cliente y el **total exacto de mensajes emitidos**: `[client_id, total_msg_count]`.

2. **Coordinador Dinámico en `Sum`**:
   - La réplica de `Sum` que extrae el `EOF` de `input_queue` asume automáticamente el rol de **Coordinador** para ese `client_id`.
   - Inicializa una estructura de coordinación con `total_expected` y registra su propio conteo de mensajes procesados localmente.
   - Si su conteo local ya iguala el total esperado (por ejemplo, si todas las frutas fueron procesadas por esta réplica o si `SUM_AMOUNT == 1`), emite directamente el `COMMIT`. De lo contrario, emite un mensaje broadcast `["PREPARE", client_id, ID]` a través de `SUM_CONTROL_EXCHANGE`.

3. **Enrutamiento Dirigido de `READY` mediante Routing Keys (`READY_{coordinator_id}`)**:
   - Cada réplica se suscribe a su cola de control privada con dos routing keys:
     - `SUM_CONTROL_EXCHANGE`: canal broadcast donde recibe `PREPARE` y `COMMIT`.
     - `f"READY_{ID}"`: canal unicast privado donde solo este nodo recibe respuestas `READY` cuando actúa como coordinador.
   - Las réplicas participantes, al recibir `PREPARE`, registran quién es el coordinador (`coordinators[client_id] = coordinator_id`) y le responden su conteo actual mediante `["READY", client_id, ID, count]`, enviándolo **exclusivamente a la routing key `f"READY_{coordinator_id}"`**. De esta forma, las respuestas no saturan a las demás réplicas.

4. **Manejo Reactivo de Mensajes Rezagados**:
   - Si una réplica de `Sum` recibe un mensaje de datos en `_process_data` después de haber enviado su `READY` inicial (un dato rezagado que estaba en tránsito), incrementa su contador local y, al verificar que `client_id in self.coordinators`, **envía automáticamente un nuevo `READY` actualizado** al coordinador con el conteo incrementado.
   - Gracias a esto, no se requieren temporizadores (*timers*), esperas activas (*busy loops*) ni sondeos periódicos. El sistema es puramente guiado por eventos.

5. **Corte y Emisión de `COMMIT`**:
   - El coordinador acumula en memoria los conteos reportados por cada nodo: `counts[sender_id] = count`.
   - Apenas la suma de conteos de todos los nodos alcanza el total esperado ($\sum \text{counts} = \text{total\_expected}$), se tiene la certeza matemática de que **todos los mensajes de datos del cliente fueron consumidos en el clúster**.
   - El coordinador emite el broadcast de `["COMMIT", client_id]` a través de `SUM_CONTROL_EXCHANGE` y vacía sus propios acumuladores (`_process_eof`).
   - Al recibir el `COMMIT`, cada réplica participante vacía sus datos acumulados hacia `Aggregation` y envía su respectivo `EOF`, completando la barrera sin pérdidas ni carreras.

---

## 3. Coordinación entre `Sum` y `Aggregation`
Una vez completado el procesamiento en `Sum`, se debe distribuir la información hacia las réplicas de `Aggregation`:
- **Particionamiento por Hash (Routing Key)**: Para evitar que todas las réplicas de `Aggregation` procesen las mismas frutas de forma redundante, en `Sum` se aplica una función de hash (`sha256(fruit) % AGGREGATION_AMOUNT`) determinando una routing key para cada fruta. Esto garantiza que todos los registros de una fruta específica siempre converjan en la misma réplica de `Aggregation`.
- **Broadcast de Fin de Datos (`EOF`)**: Dado que cada instancia de `Aggregation` desconoce de antemano qué frutas recibirá de cada `Sum`, toda réplica de `Sum` emite un mensaje `EOF` a cada una de las réplicas de `Aggregation`.
- **Barrera de Sincronización en `Aggregation`**: Cada réplica de `Aggregation` mantiene un contador `eof_client_count[client_id]`. Solo cuando alcanza `SUM_AMOUNT` (es decir, cuando todas las réplicas de `Sum` terminaron de emitir sus datos para ese cliente), procede a ordenar su subconjunto de frutas y enviar su top parcial hacia `join_queue`.

---

## 4. Coordinación entre `Aggregation` y `Join`
La etapa de `Join` unifica los resultados parciales calculados por las réplicas de `Aggregation`:
- **Recepción de Tops Parciales**: Cada réplica de `Aggregation` emite hacia `join_queue` únicamente sus `TOP_SIZE` frutas más frecuentes correspondientes a su partición.
- **Acumulación y Recorte Final**: `JoinFilter` recibe estos tops parciales y los consolida en una estructura en memoria por cliente (`fruit_top_by_client[client_id]`). Contabiliza las recepciones mediante `eof_client_count[client_id]`.
- **Emisión al Gateway**: Al alcanzar `AGGREGATION_AMOUNT`, se garantiza que todos los candidatos parciales fueron recibidos. Se ordenan los elementos consolidados por cantidad de mayor a menor, se recortan a las primeras `TOP_SIZE` posiciones y se envían a `results_queue` para que el `Gateway` devuelva la respuesta al cliente.

---

## 5. Escalabilidad del Sistema
- **Escalabilidad respecto a Clientes**:
  - El `Gateway` atiende múltiples conexiones en paralelo mediante un pool de procesos independientes.
  - Gracias al etiquetado de cada mensaje con `client_id`, no existe acoplamiento ni contención entre las consultas de diferentes clientes en las etapas de `Sum`, `Aggregation` y `Join`.
- **Escalabilidad ante Grandes Volúmenes de Datos**:
  - **Paralelismo en Ingesta (`Sum`)**: La cola compartida `input_queue` distribuye la carga entre réplicas de `Sum` de forma balanceada, permitiendo escalar horizontalmente ante un mayor flujo de registros entrantes.
  - **Distribución de Cómputo (`Aggregation`)**: El particionamiento por hash asegura que el espacio de claves (frutas) se reparta equitativamente entre los nodos de agregación, evitando cuellos de botella de memoria y optimizando los ordenamientos locales.
  - **Filtrado Temprano y Carga Constante en `Join`**: Cada réplica de `Aggregation` filtra y envía como máximo `TOP_SIZE` elementos. Por lo tanto, el volumen de datos que procesa `Join` por cliente está acotado superiormente por `AGGREGATION_AMOUNT * TOP_SIZE`, desacoplando completamente el costo de la etapa final del volumen total de registros de entrada.
- **Sobrecarga de Control y Mensajes de Coordinación**:
  - La coordinación entre réplicas de `Sum` (mediante el esquema `PREPARE` $\to$ `READY` $\to$ `COMMIT`) requiere únicamente $O(S)$ mensajes de control por cliente:
    - $1$ broadcast de `PREPARE`.
    - $(S - 1)$ respuestas dirigidas de `READY` enviadas únicamente al coordinador.
    - $1$ broadcast de `COMMIT` (más eventualmente algún `READY` adicional si hubo datos rezagados en vuelo).
  - El traspaso hacia `Aggregation` requiere $S \times A$ mensajes de `EOF` por cliente ($A$ = `AGGREGATION_AMOUNT`), y la entrega a `Join` requiere $A$ mensajes.
  - Esta sobrecarga de control depende exclusivamente de la cantidad de réplicas configuradas ($S$ y $A$) y es completamente independiente del volumen de datos del dataset ($N$), logrando una alta eficiencia a medida que el volumen de datos crece.

---

## 6. Manejo de Señales y Apagado Limpio
- **Captura de `SIGTERM` basada en el diseño del Cliente**: Siguiendo el diseño e implementación provisto en el `Client` (`client/main.py`), se registró la captura de `SIGTERM` en `Sum`, `Aggregation` y `Join` preservando el manejador previo (`self._prev_sigterm_handler`).
- **Liberación de Recursos**: Ante la recepción de la señal, cada instancia ejecuta primero su lógica de cierre e interrumpe el ciclo de consumo en RabbitMQ (`stop_consuming`), cierra ordenadamente sus canales y conexiones activas (`close`), y delega la ejecución al handler previo para finalizar limpiamente la ejecución sin dejar conexiones huérfanas.




