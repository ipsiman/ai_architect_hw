# Проектирование AI-систем: мини-проекты

Серия мини-проектов по проектированию AI-систем: от постановки задачи и архитектуры до данных и обоснования решения. Выполнены в рамках программы курса [«ИИ-архитектор» (OTUS)](https://otus.ru/lessons/ai-architect/).

## Кейсы

### TechnoMart: сервис персональных рекомендаций для ритейлера

Спроектирован AI-сервис рекомендаций и data-платформа под него: пайплайны, озеро данных, feature store.

- [02_Model_C4](02_Model_C4/hw02_c4_arch.md) - архитектура сервиса по C4 Model (контейнеры, компоненты, sequence) и OpenAPI-контракт `POST /get_recommendation`
- [05_Data_Pipelines](05_Data_Pipelines/TechnoMart_Data_Pipelines.md) - data pipeline от источников до feature store, выбор хранилищ

### Trip Assistant: умный помощник командировок

Ассистент, который сам планирует командировку: подбирает билеты и отели, сверяется с политикой компании через RAG, считает бюджет. Внутри мультиагентная система, рабочий прототип и решение по хостингу LLM.

- [03_Trip_assistant](03_Trip_assistant/README.md) - мультиагентная система (Supervisor + Workers), RAG-пайплайн, прототип на LangGraph
- [04_ADR](04_ADR/README.md) - анализ и решение по хостингу LLM: облачная модель по API или self-hosted, [pitch для CTO](04_ADR/pitch-cto.md)

## Проекты

| Проект | О чём | Стек |
|---|---|---|
| [C4-архитектура AI-сервиса](02_Model_C4/hw02_c4_arch.md) | Контейнеры, компоненты и ключевой сценарий рекомендательного сервиса, контракт интеграции | C4 Model, Mermaid, OpenAPI |
| [Мультиагентный ассистент](03_Trip_assistant/README.md) | Дизайн агентов, RAG-пайплайн и рабочий прототип | LangGraph, YandexGPT / MOCK |
| [Хостинг LLM: SaaS или self-hosted](04_ADR/README.md) | Сравнение вариантов по стоимости, приватности, качеству и сложности поддержки; ADR и pitch для CTO | TCO-анализ, взвешенная матрица |
| [Data pipeline рекомендаций](05_Data_Pipelines/TechnoMart_Data_Pipelines.md) | Поток данных от источников до feature store, выбор хранилищ, защита от training-serving skew | Kafka, S3 + Iceberg, Flink, Spark, Feast, Redis, Qdrant |

## Темы

- Архитектура: C4 Model, sequence-диаграммы, OpenAPI-контракты
- GenAI-паттерны: RAG, мультиагентные системы (Supervisor + Workers)
- Данные: ELT в озеро (S3 + Iceberg), stream и batch-обработка, feature store
- Решения: ADR, взвешенные матрицы критериев, расчёт TCO
- Безопасность: закрытый контур, 152-ФЗ, обезличивание PII

## Структура репозитория

```text
.
├── 02_Model_C4/          # TechnoMart: C4-архитектура + OpenAPI
├── 03_Trip_assistant/    # Trip Assistant: агенты + RAG + прототип
│   ├── 01_architecture.md
│   ├── 02_rag_flow.md
│   └── trip_assistant.py
├── 04_ADR/               # Trip Assistant: ADR по хостингу LLM + pitch
│   ├── adr/adr-000-llm-hosting.md
│   └── pitch-cto.md
└── 05_Data_Pipelines/    # TechnoMart: data pipeline и хранилища
```

