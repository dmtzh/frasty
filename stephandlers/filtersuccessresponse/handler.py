from dataclasses import dataclass

from shared.completedresult import CompletedWith
from stepdefinitions.shared import HttpResponseData

@dataclass(frozen=True)
class FilterSuccessResponseCommand:
    response: HttpResponseData

def check_success_response(response: HttpResponseData):
    match response.status_code:
        case 200:
            return response
        case _:
            return None

def handle(cmd: FilterSuccessResponseCommand):
    opt_response = check_success_response(cmd.response)
    match opt_response:
        case None:
            err_msg = f"Expected success response code (200) but got {cmd.response.status_code}"
            result = CompletedWith.Error(err_msg)
        case succ_resp:
            data = HttpResponseData.to_dict(succ_resp)
            result = CompletedWith.Data(data=data)
    return result