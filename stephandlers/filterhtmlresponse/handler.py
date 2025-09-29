from dataclasses import dataclass

from shared.completedresult import CompletedWith
from stepdefinitions.shared import HttpResponseData
from stepdefinitions.html import HtmlContentData

@dataclass(frozen=True)
class FilterHtmlResponseCommand:
    response: HttpResponseData

def check_html_response(response: HttpResponseData):
    match response.content_type:
        case "text/html":
            return HtmlContentData(response.content)
        case _:
            return None

def handle(cmd: FilterHtmlResponseCommand):
    opt_html = check_html_response(cmd.response)
    match opt_html:
        case None:
            err_msg = f"Expected html response but got {cmd.response.content_type}"
            result = CompletedWith.Error(err_msg)
        case html_resp:
            data = HtmlContentData.to_dict(html_resp)
            result = CompletedWith.Data(data=data)
    return result