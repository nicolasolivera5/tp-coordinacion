Redactar un breve informe en el archivo `INFORME.md` explicando el modo en que se coordinan las instancias de Sum y Aggregation, así como el modo en el que el sistema escala respecto a los clientes, grándes volúmens de datos y la cantidad de controles.

# Informe de Coordinación de Sistemas Distribuidos
---

## 1. Aislamiento Multi-Cliente (Multi-Client Isolation)
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
- **Exclusión Mutua (`threading.RLock`)**: Se sincronizó el procesamiento del mensaje en el hilo principal (`process_data_message`) con el callback del hilo de control (`_process_control_message`). Si un `Sum` se encuentra computando una fruta cuando llega el aviso de control, el hilo de control espera a que finalice la suma antes de vaciar el acumulador y emitir el `EOF`.

