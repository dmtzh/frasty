from dataclasses import dataclass
import functools

from expression.collections.block import Block
from expression.extra.result.traversable import traverse
from parsel import Selector

from shared.completedresult import CompletedWith
from shared.customtypes import Error
from shared.utils.asyncresult import ex_to_error_result
from stepdefinitions.html import GetLinksFromHtmlConfig

@dataclass(frozen=True)
class GetLinksFromHtmlCommand:
    config: GetLinksFromHtmlConfig
    html: dict | list

def get_links(config: GetLinksFromHtmlConfig, html: dict | list):
    def link_selector_to_dict(text_name: str, link_name: str, link: Selector):
        res = {
            text_name: link.css("a::text").get(),
            link_name: link.css("a::attr(href)").get()
        }
        return res
    @ex_to_error_result(Error.from_exception)
    def get_from_content(content: dict):
        content_selector = Selector(text=content["content"])
        links = content_selector.css("a")
        text_name = config.text_name or "text"
        link_name = config.link_name or "link"
        html_without_content = {k:v for k, v in content.items() if k != "content"}
        link_to_dict = functools.partial(link_selector_to_dict, text_name, link_name)
        res = [html_without_content | link_to_dict(link) for link in links]
        return res
    list_of_content = html if isinstance(html, list) else [html]
    contents_res = traverse(get_from_content, Block(list_of_content))
    res = contents_res.map(lambda contents: functools.reduce(lambda acc, curr: acc + curr, contents, []))
    return res

def handle(cmd: GetLinksFromHtmlCommand):
    # 1. get links from html
    links_res = get_links(cmd.config, cmd.html)
    # 2. return result
    result_res = links_res\
        .map(lambda links: CompletedWith.Data(links) if links else CompletedWith.NoData())\
        .map_error(lambda error: CompletedWith.Error(str(error)))
    result = result_res.merge()
    return result