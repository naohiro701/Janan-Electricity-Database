from __future__ import annotations

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
import uvicorn

from apps.api.routes.generation_unit import router as generation_unit_router
from apps.api.routes.intertie_flow import router as intertie_flow_router
from apps.api.routes.intraday import router as intraday_router
from apps.api.routes.reserve_ratio import router as reserve_ratio_router
from apps.api.routes.spot import router as spot_router
from apps.api.routes.trunk_flow import router as trunk_flow_router


app = FastAPI(
    title="Power Data Platform PoC API",
    version="0.1.0",
    description="Reference API for Japanese public power data PoC.",
)


@app.get("/", response_class=HTMLResponse, tags=["home"])
def home() -> str:
    return """
    <html>
      <head><title>Power Data Platform PoC</title></head>
      <body>
        <h1>Power Data Platform PoC</h1>
        <p>FastAPI reference UI is available at <a href="/docs">/docs</a>.</p>
        <ul>
          <li><a href="/spot">/spot</a></li>
          <li><a href="/intraday">/intraday</a></li>
          <li><a href="/reserve-ratio">/reserve-ratio</a></li>
          <li><a href="/intertie-flow">/intertie-flow</a></li>
          <li><a href="/trunk-flow">/trunk-flow</a></li>
          <li><a href="/generation-unit">/generation-unit</a></li>
        </ul>
      </body>
    </html>
    """


app.include_router(spot_router)
app.include_router(intraday_router)
app.include_router(reserve_ratio_router)
app.include_router(intertie_flow_router)
app.include_router(trunk_flow_router)
app.include_router(generation_unit_router)


def run() -> None:
    uvicorn.run("apps.api.main:app", host="0.0.0.0", port=8000, reload=False)
