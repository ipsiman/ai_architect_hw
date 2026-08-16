"""
ДЗ-03. «Умный помощник командировок» — прототип мультиагентной системы с RAG.

Курс «AI-архитектор», занятие 7.
Задание: агент «Менеджер» делегирует задачу агенту «Поисковику» и возвращает ответ.

Паттерн: иерархия — supervisor (Менеджер, Plan-and-Execute) + worker (Поисковик, ReAct).
Инструменты Поисковика:
  - rag_search_policy — мини-RAG по политике командировок;
  - search_flights, search_hotels — заглушки внешних API.

Запуск:
# MOCK-режим, ключи не нужны
  python trip_assistant.py
# реальный YandexGPT или прописать свои ключи в коде
  YANDEX_API_KEY=... YANDEX_FOLDER_ID=... python trip_assistant.py

Полная архитектура и production-пайплайн RAG — в документах:
  01_architecture.md, 02_rag_flow.md.
"""

import hashlib
import math
import os
import re

from langchain_core.embeddings import Embeddings
from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.tools import tool
from langchain_core.vectorstores import InMemoryVectorStore
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langgraph.graph import END, MessagesState, START, StateGraph
from langgraph.prebuilt import create_react_agent
from langgraph.types import Command

# =====================================================================
# 1. КОНФИГУРАЦИЯ
# =====================================================================

YANDEX_API_KEY = os.getenv("YANDEX_API_KEY", "")
YANDEX_FOLDER_ID = os.getenv("YANDEX_FOLDER_ID", "")
YANDEX_MODEL = os.getenv("YANDEX_MODEL", "yandexgpt-lite")
YANDEX_BASE_URL = os.getenv("YANDEX_BASE_URL", "https://ai.api.cloud.yandex.net/v1")

# Нет ключей -> MOCK_MODE: граф работает на заглушках LLM,
# обмен сообщениями между агентами полностью сохраняется.
MOCK_MODE = not (YANDEX_API_KEY and YANDEX_FOLDER_ID)


def _trace(node: str, text: str) -> None:
    print(f"  [{node}] {text}")


def _yandex_model_uri(model_name: str, folder_id: str) -> str:
    """Короткое имя модели -> URI вида gpt://<folder>/<model>/latest"""
    if model_name.startswith("gpt://"):
        return model_name
    normalized = model_name.strip("/")
    if "/" not in normalized:
        normalized = f"{normalized}/latest"
    return f"gpt://{folder_id}/{normalized}"


def _build_llm():
    if MOCK_MODE:
        return None
    from langchain_openai import ChatOpenAI

    return ChatOpenAI(
        model=_yandex_model_uri(YANDEX_MODEL, YANDEX_FOLDER_ID),
        api_key=YANDEX_API_KEY,
        base_url=YANDEX_BASE_URL,
        default_headers={"OpenAI-Project": YANDEX_FOLDER_ID},
        temperature=0,
        request_timeout=60,
        max_retries=2,
    )


llm_manager = _build_llm()  # LLM агента «Менеджер»
llm_worker = _build_llm()   # LLM агента «Поисковик»

# =====================================================================
# 2. МИНИ-RAG: документы -> чанкинг -> эмбеддинг -> Vector DB -> retrieval -> реранжинг
#    Production-пайплайн (pgvector, гибридный поиск, кросс-энкодер) — в 02_rag_flow.md
# =====================================================================

# --- 2.1. Корпус: «Политика командировок» (в production — PDF/Confluence через ETL) ---
POLICY_SECTIONS = [
    "§1. Общие правила. Командировка оформляется через подсистему «Умный помощник» не позднее чем за 3 рабочих дня до даты выезда.",
    "§2. Класс обслуживания. Сотрудникам до уровня «менеджер» включительно — эконом-класс; руководителям — бизнес-класс на рейсах длиннее 4 часов.",
    "§3. Лимиты на авиабилеты, эконом-класс: Москва — Казань до 15 000 руб.; Москва — Санкт-Петербург до 12 000 руб.; Москва — Новосибирск до 28 000 руб.",
    "§4. Лимиты на отели: Москва до 8 000 руб./ночь; Казань до 6 000 руб./ночь; Санкт-Петербург до 7 000 руб./ночь; другие города России до 5 500 руб./ночь.",
    "§5. Суточные по России: 3 500 руб. в день; при поездке короче суток — 50% суточных.",
    "§6. Требования к отелю: категория не выше 4 звёзд, рейтинг не ниже 8.0, пешая доступность от места работы либо центр города.",
    "§7. Трансфер: такси от/до аэропорта компенсируется до 2 500 руб. в одну сторону.",
    "§8. Исключения: превышение лимита возможно только с письменного одобрения финансового директора, полученного до выезда.",
    "§9. Страховка: для командировок по России полис не требуется; для международных поездок обязателен.",
    "§10. Отчётность: авансовый отчёт со всеми чеками сдаётся в течение 5 рабочих дней после возвращения.",
]

# --- 2.2. Чанкинг ---
_splitter = RecursiveCharacterTextSplitter(
    chunk_size=300,
    chunk_overlap=50,
    separators=["\n\n", "\n", ". ", " ", ""],
)
POLICY_CHUNKS = _splitter.split_text("\n\n".join(POLICY_SECTIONS))

# --- 2.3. Эмбеддинги без внешних API: bag-of-hashed-words + L2-нормировка ---
_TOKEN_RE = re.compile(r"[а-яёa-z0-9]+")


def _tokens(text: str) -> list:
    return _TOKEN_RE.findall(text.lower())


def hash_embed(text: str, dim: int = 256) -> list:
    vec = [0.0] * dim
    for token in _tokens(text):
        h = int(hashlib.md5(token.encode("utf-8")).hexdigest(), 16)
        vec[h % dim] += 1.0 if (h >> 8) % 2 == 0 else -1.0
    norm = math.sqrt(sum(v * v for v in vec)) or 1.0
    return [v / norm for v in vec]


class HashEmbeddings(Embeddings):
    """Детерминированные эмбеддинги для демо (в production — YandexGPT Embeddings API)."""

    def embed_documents(self, texts):
        return [hash_embed(t) for t in texts]

    def embed_query(self, text):
        return hash_embed(text)


# --- 2.4. Vector DB (в production — Yandex Managed PostgreSQL + pgvector, HNSW) ---
policy_store = InMemoryVectorStore(HashEmbeddings())
policy_store.add_texts(POLICY_CHUNKS)


# --- 2.5-2.6. Retrieval + реранжинг (эвристика перекрытия терминов вместо кросс-энкодера) ---
def _overlap_score(query: str, chunk: str) -> float:
    q, c = set(_tokens(query)), set(_tokens(chunk))
    union = q | c
    return len(q & c) / len(union) if union else 0.0


def retrieve_policy(query: str, k: int = 5, top_n: int = 3) -> list:
    found = policy_store.similarity_search_with_score(query, k=k)
    max_vec = max((score for _, score in found), default=1.0) or 1.0
    reranked = sorted(
        (
            (doc.page_content, 0.6 * (score / max_vec) + 0.4 * _overlap_score(query, doc.page_content))
            for doc, score in found
        ),
        key=lambda item: item[1],
        reverse=True,
    )
    return reranked[:top_n]


# --- 2.7. RAG как инструмент агента ---
@tool
def rag_search_policy(query: str) -> str:
    """Поиск по политике командировок компании: лимиты на билеты и отели, суточные, правила бронирования."""
    results = retrieve_policy(query)
    if not results:
        return "В политике ничего не найдено — эскалируй вопрос travel-менеджеру."
    return "\n".join(f"• {text} (релевантность {score:.2f})" for text, score in results)


# =====================================================================
# 3. ИНСТРУМЕНТЫ ПОИСКОВИКА (заглушки внешних API: в production — GDS/TMC и OTA)
# =====================================================================


@tool
def search_flights(origin: str, destination: str, date: str) -> str:
    """Поиск авиабилетов: до 3 вариантов с временем вылета и ценой в рублях."""
    options = [
        "Рейс SU 1200, 08:10 → 09:55, эконом, 9 800 руб.",
        "Рейс DP 214, 12:40 → 14:25, эконом, 8 200 руб.",
        "Рейс SU 1204, 18:30 → 20:15, эконом, 11 400 руб.",
    ]
    return f"Билеты {origin} → {destination} на {date}:\n" + "\n".join(f"- {o}" for o in options)


@tool
def search_hotels(city: str, checkin: str, nights: int) -> str:
    """Поиск отелей: до 3 вариантов со звёздностью, рейтингом и ценой за ночь."""
    options = [
        "Отель «Казань Центр», 4 звезды, рейтинг 8.7, 5 400 руб./ночь, 900 м до Кремля",
        "Отель «Ривьер», 3 звезды, рейтинг 8.2, 4 100 руб./ночь, 2.5 км до центра",
        "Отель «Мирибель», 4 звезды, рейтинг 8.9, 5 900 руб./ночь, центр города",
    ]
    return f"Отели: {city}, заезд {checkin}, ночей — {nights}:\n" + "\n".join(f"- {o}" for o in options)


# =====================================================================
# 4. АГЕНТЫ
#    Общий state: messages + next.
#    Менеджер: Command(goto="searcher") с задачей -> Поисковик: ReAct с инструментами
#    -> Command(goto="manager") с отчётом -> Менеджер: goto=END с итоговым ответом.
# =====================================================================


class TripState(MessagesState):
    """Общий state мультиагентной системы: история сообщений + следующий агент."""

    next: str = ""


# ---------------- Агент «Поисковик» (ReAct: рассуждение -> инструменты -> ответ) ----------------
SEARCHER_PROMPT = """Ты — агент «Поисковик» подсистемы оформления командировок.
Тебе приходит задача от агента «Менеджер». Выполни её строго по шагам:
1. Вызови rag_search_policy и выясни лимиты компании для этой поездки.
2. Вызови search_flights для поиска билетов: город вылета, город прилёта, дата.
3. Вызови search_hotels для поиска отеля: город, дата заезда, число ночей.
4. Верни Менеджеру краткий отчёт: лимиты по политике, лучшие варианты билетов и отеля
   и укладываются ли они в лимиты. Не задавай вопросов — действуй.
"""

searcher_agent = None if MOCK_MODE else create_react_agent(
    llm_worker,
    tools=[rag_search_policy, search_flights, search_hotels],
    prompt=SEARCHER_PROMPT,
)


def _last_manager_instruction(state: TripState) -> str:
    for msg in reversed(state["messages"]):
        if isinstance(msg, HumanMessage) and getattr(msg, "name", "") == "manager":
            return msg.content
    return str(state["messages"][0].content)


def searcher_node(state: TripState) -> Command:
    """Поисковик: выполняет задачу Менеджера и возвращает отчёт в общий state."""
    _trace("searcher", "получил задачу от Менеджера")
    instruction = _last_manager_instruction(state)

    if MOCK_MODE:
        # Демонстрация без LLM: вызываем те же инструменты, что вызвал бы ReAct-агент
        policy = rag_search_policy.invoke({"query": "лимиты на билеты и отели командировка"})
        flights = search_flights.invoke({"origin": "Москва", "destination": "Казань", "date": "2026-09-12"})
        hotels = search_hotels.invoke({"city": "Казань", "checkin": "2026-09-12", "nights": 3})
        _trace("searcher", "вызвал инструменты: rag_search_policy, search_flights, search_hotels")
        report = (
            "Отчёт Поисковика по задаче Менеджера.\n\n"
            f"Лимиты по политике:\n{policy}\n\n{flights}\n\n{hotels}\n\n"
            "Вывод: лучший вариант — рейс SU 1200 за 9 800 руб. (лимит 15 000 руб.) "
            "и отель «Казань Центр» за 5 400 руб./ночь (лимит 6 000 руб./ночь). "
            "Все варианты в рамках политики."
        )
    else:
        result = searcher_agent.invoke({"messages": [HumanMessage(content=instruction, name="manager")]})
        tool_calls = sum(1 for m in result["messages"] if m.type == "tool")
        _trace("searcher", f"ReAct-цикл завершён, вызовов инструментов: {tool_calls}")
        report = str(result["messages"][-1].content)

    return Command(
        goto="manager",
        update={
            "next": "manager",
            "messages": state["messages"] + [AIMessage(content=report, name="searcher")],
        },
    )


# ---------------- Агент «Менеджер» (supervisor: план -> делегирование -> контроль) ----------------
MANAGER_PROMPT = """Ты — «Менеджер командировок», супервизор мультиагентной системы.
В твоём распоряжении агент «Поисковик» (next="searcher"): он ищет билеты, отели
и лимиты по политике компании. Сам ты ничего не ищешь — планируешь и делегируешь.

Правила маршрутизации:
- Если отчёта Поисковика в истории ещё нет — next="searcher", а в поле instruction
  сформулируй чёткое задание: города, даты, что найти.
- Если отчёт Поисковика уже есть — next="FINISH", а в поле instruction напиши
  итоговый ответ сотруднику: план командировки, выбранные варианты, итоговая смета
  и отметка о соответствии политике. Отвечай по-русски, кратко и структурированно.
"""


def _searcher_reported(state: TripState) -> bool:
    return any(
        isinstance(m, AIMessage) and getattr(m, "name", "") == "searcher"
        for m in state["messages"]
    )


def manager_node(state: TripState) -> Command:
    """Менеджер: делегирует задачу Поисковику, затем формирует итоговый ответ."""
    _trace("manager", "анализирую состояние диалога")
    searcher_done = _searcher_reported(state)

    if MOCK_MODE:
        if not searcher_done:
            instruction = (
                "Задача для Поисковика: командировка Москва → Казань, вылет 2026-09-12, "
                "возвращение 2026-09-15 (3 ночи). Найди лимиты по политике, билеты и отель."
            )
            _trace("manager", "делегирует задачу Поисковику")
            return Command(
                goto="searcher",
                update={
                    "next": "searcher",
                    "messages": state["messages"]
                    + [HumanMessage(content=f"[ЗАДАЧА ОТ МЕНЕДЖЕРА]\n{instruction}", name="manager")],
                },
            )
        final = (
            "Командировка спланирована (MOCK-режим, данные заглушек):\n"
            "• Перелёт: SU 1200 Москва → Казань, 12.09, 9 800 руб. (лимит 15 000 руб. — в рамках)\n"
            "• Отель: «Казань Центр», 3 ночи по 5 400 руб. (лимит 6 000 руб./ночь — в рамках)\n"
            "• Итого транспорт и проживание: 26 000 руб., плюс суточные 10 500 руб.\n"
            "Вывод: в рамках политики, направляю на согласование руководителю."
        )
        _trace("manager", "формирует итоговый ответ")
        return Command(
            goto=END,
            update={
                "next": END,
                "messages": state["messages"] + [AIMessage(content=final, name="manager")],
            },
        )

    # --- Реальный режим: маршрутизация через structured output ---
    from typing_extensions import TypedDict

    class Router(TypedDict):
        next: str
        instruction: str

    history = "\n\n".join(f"{getattr(m, 'name', m.type)}: {m.content}" for m in state["messages"])
    response = llm_manager.with_structured_output(Router, method="function_calling").invoke(
        [HumanMessage(content=f"{MANAGER_PROMPT}\n\nИстория диалога:\n{history}")]
    )
    goto = response.get("next")
    if goto not in ("searcher", "FINISH"):
        goto = "FINISH"
    if searcher_done:
        goto = "FINISH"  # защита от зацикливания: отчёт Поисковика уже есть

    if goto == "searcher":
        _trace("manager", "делегирует задачу Поисковику")
        return Command(
            goto="searcher",
            update={
                "next": "searcher",
                "messages": state["messages"]
                + [HumanMessage(content=f"[ЗАДАЧА ОТ МЕНЕДЖЕРА]\n{response['instruction']}", name="manager")],
            },
        )

    _trace("manager", "формирует итоговый ответ")
    return Command(
        goto=END,
        update={
            "next": END,
            "messages": state["messages"] + [AIMessage(content=response["instruction"], name="manager")],
        },
    )


# =====================================================================
# 5. ГРАФ LANGGRAPH: START -> manager <-> searcher -> END
# =====================================================================


def build_graph():
    workflow = StateGraph(TripState)
    workflow.add_node("manager", manager_node)    # Менеджер (supervisor)
    workflow.add_node("searcher", searcher_node)  # Поисковик (ReAct-воркер)
    workflow.add_edge(START, "manager")
    return workflow.compile()


graph = build_graph()

# =====================================================================
# 6. ДЕМОНСТРАЦИЯ
# =====================================================================


def run_demo(user_request: str):
    """Прогон сценария: видно, как агенты обмениваются сообщениями через общий state."""
    initial_state = {"messages": [HumanMessage(content=user_request, name="user")]}

    print("=" * 72)
    print("Запрос сотрудника:", user_request)
    print("=" * 72)
    print("\nШаги агентов:")
    last_state = initial_state
    for state in graph.stream(initial_state, config={"recursion_limit": 30}, stream_mode="values"):
        last_state = state

    print("\n" + "=" * 72)
    print("Обмен сообщениями между агентами (общий state):")
    print("=" * 72)
    for msg in last_state["messages"]:
        speaker = getattr(msg, "name", None) or msg.type
        content = str(msg.content).strip()
        print(f"\n--- [{speaker}] ---")
        print(content if len(content) <= 600 else content[:600] + "\n...")

    print("\n" + "=" * 72)
    print("ИТОГОВЫЙ ОТВЕТ МЕНЕДЖЕРА СОТРУДНИКУ:")
    print("=" * 72)
    print(last_state["messages"][-1].content)


if __name__ == "__main__":
    print(f"Режим работы: {'MOCK (ключи YandexGPT не заданы)' if MOCK_MODE else 'YandexGPT'}")
    print(f"Мини-RAG: проиндексировано чанков политики — {len(POLICY_CHUNKS)}\n")

    run_demo(
        "Оформи командировку в Казань с 12 по 15 сентября: "
        "нужен билет из Москвы и отель в центре."
    )
