from expression import Result
from fastapi import HTTPException
from fastapi.responses import FileResponse

from shared.customtypes import DefinitionIdValue
from shared.definitionsstore import definitions_storage
from shared.dtodefinition import DefinitionAdapter as LegacyDefinitionAdapter
from shared.utils.asyncresult import async_catch_ex
from shared.utils.result import ResultTag

import adddefinitionapihandler
from config import app

@app.get("/tickets")
def tickets():
    return FileResponse("./html_sources/get_ticket.html")

@app.post("/definitions")
async def add_definition(request: adddefinitionapihandler.AddDefinitionRequest):
    return await adddefinitionapihandler.handle(request)

@app.get("/definitions/legacy/{id}")
async def legacy_get_definition(id: str):
    opt_def_id = DefinitionIdValue.from_value_with_checksum(id)
    if opt_def_id is None:
        raise HTTPException(status_code=404)
    opt_definition_with_ver_res = await async_catch_ex(definitions_storage.get_with_ver)(opt_def_id)
    match opt_definition_with_ver_res:
        case Result(ResultTag.OK, ok=None):
            raise HTTPException(status_code=404)
        case Result(ResultTag.OK, ok=(definition, _)):
            definition_dto = LegacyDefinitionAdapter.to_list(definition)
            return definition_dto
        case _:
            raise HTTPException(status_code=503, detail="Oops... Service temporary unavailable, please try again later.")
