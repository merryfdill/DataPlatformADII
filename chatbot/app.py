"""ADII chatbot backend (Phase 2.24, LLM swapped to Gemini in Phase 2.25).

Read-only Q&A layer over the Gold tables. The LLM (Gemini) never sees raw
Trino access - it can only call the fixed tools in tools.py (unchanged),
which build validated SELECT statements against the 4 allow-listed Gold
tables. The NORMAL/MINORE/MAJORE verdict is never recomputed here: it is
read back exactly as produced by spark/jobs/arbitrage_gold.py (Phase 2.21)
and exposed through dbt (Phase 2.22).
"""

import logging
import os

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from google import genai
from google.genai import errors as genai_errors
from google.genai import types
from pydantic import BaseModel

import tools

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("chatbot")

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
if not GEMINI_API_KEY:
    raise RuntimeError("GEMINI_API_KEY manquante dans l'environnement (.env).")
MODEL = os.environ.get("GEMINI_MODEL", "gemini-3.6-flash")

client = genai.Client(api_key=GEMINI_API_KEY)

SYSTEM_PROMPT = """Tu es l'assistant ADII, un chatbot d'aide a l'analyse des declarations
douanieres marocaines (projet de simulation/prototype - donnees BADR simulees).

REGLES ABSOLUES :
- Tu n'as PAS le droit de calculer, deviner ou inventer un ratio, une valeur, un verdict
  d'arbitrage ou tout autre chiffre. Toute statistique que tu donnes DOIT provenir d'un
  appel a un outil (tool). N'invente jamais une donnee absente des resultats d'outil.
- Le verdict NORMAL / MINORE / MAJORE est deja calcule par un pipeline Spark deterministe
  (seuils P10/P90 par CODE_NGP, methode documentee dans docs/arbitrage_gold.md) AVANT que tu
  n'intervienne. Tu ne dois jamais le recalculer, le contester ou en proposer un autre - tu
  peux uniquement l'expliquer avec les donnees disponibles.
- Les seuils exacts (P10/P90) ne sont pas stockes dans Gold. N'affirme jamais un chiffre de
  seuil precis. Pour situer une declaration, compare uniquement son RATIO_UNITAIRE au ratio
  moyen/median reel de sa categorie (deja fournis par expliquer_declaration).
- Si une question sort du perimetre du projet (donnee absente de Gold, sujet sans rapport
  avec les declarations douanieres/l'arbitrage), decline poliment et explique le perimetre.
- Reponds toujours en francais, de maniere claire et concise.
- Quand tu donnes un chiffre, mentionne la table source (ex: "Source : iceberg.gold.mart_arbitrage_kpi").
- CODE_NGP possibles : 85171300 (Smartphone), 84713000 (PC Portable), 85287200 (Televiseur).
- N'appelle JAMAIS un outil avec une valeur d'exemple, un placeholder ou un identifiant inconnu
  (ex: badr_id fictif). Si tu n'as pas de valeur concrete pour un parametre obligatoire, ne pas
  appeler l'outil : explique plutot en texte ce qu'il faudrait fournir (ex: un BADR_ID reel).

Utilise les outils a ta disposition pour recuperer les donnees necessaires avant de repondre
a toute question factuelle."""


def _build_gemini_tool() -> types.Tool:
    """Converts the existing OpenAI-style tools.TOOLS_SCHEMA (unchanged) into
    Gemini FunctionDeclarations, via parameters_json_schema which accepts a
    plain JSON-schema dict directly - no need to touch tools.py.
    """
    declarations = []
    for entry in tools.TOOLS_SCHEMA:
        fn = entry["function"]
        declarations.append(types.FunctionDeclaration(
            name=fn["name"],
            description=fn.get("description", ""),
            parameters_json_schema=fn.get("parameters", {"type": "object", "properties": {}}),
        ))
    return types.Tool(function_declarations=declarations)


GEMINI_TOOL = _build_gemini_tool()

app = FastAPI(title="ADII Chatbot")


class ChatMessage(BaseModel):
    role: str
    content: str


class ChatRequest(BaseModel):
    message: str
    history: list[ChatMessage] = []


class ToolCallInfo(BaseModel):
    tool: str
    args: dict
    sql: str | None = None
    source: str
    error: str | None = None


class ChatResponse(BaseModel):
    reply: str
    tool_calls: list[ToolCallInfo] = []


MAX_TOOL_ROUNDS = 4

_FALLBACK_LLM_ERROR = (
    "Desole, une erreur est survenue lors de l'appel au LLM. Reformulez votre question, s'il vous plait."
)


def _to_gemini_role(role: str) -> str:
    return "model" if role == "assistant" else "user"


@app.get("/api/health")
def health():
    return {"status": "ok", "model": MODEL}


@app.post("/api/chat", response_model=ChatResponse)
def chat(req: ChatRequest):
    contents: list[types.Content] = [
        types.Content(role=_to_gemini_role(m.role), parts=[types.Part(text=m.content)])
        for m in req.history
    ]
    contents.append(types.Content(role="user", parts=[types.Part(text=req.message)]))

    config = types.GenerateContentConfig(
        system_instruction=SYSTEM_PROMPT,
        tools=[GEMINI_TOOL],
        temperature=0.2,
    )

    executed: list[ToolCallInfo] = []

    for _round in range(MAX_TOOL_ROUNDS):
        try:
            resp = client.models.generate_content(model=MODEL, contents=contents, config=config)
        except genai_errors.APIError as exc:
            logger.warning("Gemini API error: %s", exc)
            return ChatResponse(reply=_FALLBACK_LLM_ERROR, tool_calls=executed)
        except Exception as exc:  # network errors, unexpected SDK failures, etc.
            logger.warning("Unexpected error calling Gemini: %s", exc)
            return ChatResponse(reply=_FALLBACK_LLM_ERROR, tool_calls=executed)

        if not resp.candidates or resp.candidates[0].content is None:
            logger.warning("Gemini returned no usable candidate (reponse possiblement filtree).")
            return ChatResponse(reply=_FALLBACK_LLM_ERROR, tool_calls=executed)

        function_calls = resp.function_calls or []

        if not function_calls:
            reply_text = resp.text or ""
            return ChatResponse(reply=reply_text, tool_calls=executed)

        contents.append(resp.candidates[0].content)

        response_parts = []
        for fc in function_calls:
            name = fc.name or ""
            args = fc.args if isinstance(fc.args, dict) else {}

            fn = tools.TOOL_FUNCTIONS.get(name)
            error = None
            sql_used = None
            if fn is None:
                payload = {"error": f"Outil inconnu : {name}"}
                error = payload["error"]
            else:
                try:
                    result = fn(args)
                    payload = result
                    sql_used = result.get("sql")
                except Exception as exc:
                    logger.warning("Tool %s failed: %s", name, exc)
                    payload = {"error": str(exc)}
                    error = str(exc)

            executed.append(ToolCallInfo(
                tool=name,
                args=args,
                sql=sql_used,
                source=tools.TOOL_SOURCE_TABLE.get(name, "inconnu"),
                error=error,
            ))

            response_parts.append(types.Part(function_response=types.FunctionResponse(
                name=name,
                response=payload if isinstance(payload, dict) else {"result": payload},
            )))

        contents.append(types.Content(role="user", parts=response_parts))

    return ChatResponse(
        reply="Desole, je n'ai pas pu terminer le traitement de cette question (trop d'etapes). Reformulez, s'il vous plait.",
        tool_calls=executed,
    )


app.mount("/", StaticFiles(directory="static", html=True), name="static")
