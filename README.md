# natscracker

## Overview

Данный пакет позволяет реализовать event-driven архитектуру совместно с [NATS](https://docs.nats.io/) и трейсингом OTLP.

Для обмена сообщениями используется protobuf и базовая структура `BaseEvent`, которая содержит в себе необходимый payload.

При проведении тестов OTLP в качестве трейсинга использовался [Jaeger](https://github.com/jaegertracing/jaeger-ui).

## Features

- Обмен сообщениями в формате protobuf
- Единая структура для обмена данными
- Возможность масштабирования увеличивая количество consumers и workers
- Публикация сообщений в NATS
- Интеграция OTLP
- Graceful shutdown, пока workers не обработают сообщения, которые они обрабатывают в данный момент времени, программа не будет завершена
- Возможность повтора тех сообщений, при отработке которых произошла ошибка, а также их сохранение для дальнейшего изучения, при исчерпывании количества повторов.

## Under development

### TO DO:
- [ ] Implement retries on failed messages
- [X] Managing consumers depending on working policies
- [X] FIX: When adding more than one consumer only one subscription initialized
- [X] Implement full DI support: logging, error handling, etc.
- [X] Ability to make a service that just publishes messages
