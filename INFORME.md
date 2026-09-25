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
En la etapa de `Sum` todas las instancias compiten como consumidores de la misma cola compartida de entrada (`input_queue`). Lo que genera dos problemas:
1. **Notificación de fin de datos única**: Los mensajes de datos de un cliente se reparten entre las distintas réplicas de `Sum`, pero el mensaje de `EOF` es tomado por solo una de ellas desde la cola `input_queue`.
2. **Condiciones de carrera y desincronización**: Las réplicas que no recibieron el `EOF` directamente deben enterarse de la finalización del cliente para vaciar sus acumuladores, pero sin adelantarse a los mensajes de datos que aún tienen en proceso.

### 2.1 Mecanismo de Coordinación Implementado:
- **Exchange de Control (`SUM_CONTROL_EXCHANGE`)**: Se configuró un exchange de tipo topic al que cada instancia de `Sum` se suscribe con una cola exclusiva en un hilo consumidor dedicado.
- **Broadcast de EOF**: La réplica de `Sum` que consume el `EOF` original desde `input_queue` procesa sus frutas acumuladas, emite sus totales y su `EOF` hacia `Aggregation`, y luego publica una notificación en `SUM_CONTROL_EXCHANGE`.
- **Recepción Sincronizada en Réplicas**: Las demás réplicas reciben la notificación en su hilo de control y proceden a emitir sus acumulados y su respectivo `EOF` hacia `Aggregation`.
- **Procesamiento doble**: Para evitar procesamientos dobles (en particular si una réplica recibe su propio mensaje de broadcast), cada réplica registra los clientes finalizados en un conjunto (`processed_eof_clients`) y descarta notificaciones repetidas.

### 2.2 Resolución de Condiciones de Carrera:
- **Prefetch Unitario (`prefetch_count=1`)**: Por defecto, RabbitMQ entrega mensajes en ráfagas al buffer TCP del cliente. Sin límite de prefetch, un `Sum` podía tener datos en su memoria local aún no procesados cuando le llegaba el aviso de control, provocando pérdidas de datos. Con `basic_qos(prefetch_count=1)`, RabbitMQ entrega estrictamente un mensaje a la vez por consumidor, garantizando que no existan mensajes ocultos en buffers locales.
- **Exclusión Mutua (`threading.RLock`)**: Se sincronizó el procesamiento del mensaje en el hilo principal (`_process_data_messsage`) con el callback del hilo de control (`_process_control_message`). Si un `Sum` se encuentra computando una fruta cuando llega el aviso de control, el hilo de control espera a que finalice la suma antes de vaciar el acumulador y emitir el `EOF`.

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
  - La coordinación entre réplicas de `Sum` utiliza un exchange topic dedicado con $O(S)$ mensajes por cliente (donde $S$ es `SUM_AMOUNT`).
  - El traspaso hacia `Aggregation` requiere $S \times A$ mensajes de `EOF` por cliente ($A$ = `AGGREGATION_AMOUNT`), y la entrega a `Join` requiere $A$ mensajes.
  - Esta sobrecarga de control depende exclusivamente de la cantidad de réplicas configuradas y es completamente independiente del volumen de datos del dataset ($N$), logrando una alta eficiencia a medida que el volumen de datos crece.

---

## 6. Manejo de Señales y Apagado Limpio
- **Captura de `SIGTERM` basada en el diseño del Cliente**: Siguiendo el diseño e implementación provisto en el `Client` (`client/main.py`), se registró la captura de `SIGTERM` en `Sum`, `Aggregation` y `Join` preservando el manejador previo (`self._prev_sigterm_handler`).
- **Liberación de Recursos**: Ante la recepción de la señal, cada instancia ejecuta primero su lógica de cierre e interrumpe el ciclo de consumo en RabbitMQ (`stop_consuming`), cierra ordenadamente sus canales y conexiones activas (`close`), y delega la ejecución al handler previo para finalizar limpiamente la ejecución sin dejar conexiones huérfanas.




