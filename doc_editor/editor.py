from pathlib import Path

from odf.opendocument import load
from odf import text, teletype
from odf.text import P, Tab, Span
from odf.style import Style, TextProperties


ROOT_DIR = Path(__file__).parent


class DocEditor:
    def __init__(self, path: str | Path) -> None:
        self.doc = load(path)

    def searchNReplace(
        self,
        search: str,
        replaces: list[str],
        add_tab: bool = False,
        font_size: int = 13,
    ) -> None:
        if len(replaces) < 0:
            raise ValueError("List of replaces should be > 0")

        replace_index = 0
        text_elements = self.doc.getElementsByType(text.P)

        for i, element in enumerate(text_elements):
            text_content = teletype.extractText(element)
            if text_content.strip() == search:
                new_element = P()

                if add_tab:
                    tab = Tab()
                    new_element.addElement(tab)

                replace_text = None
                if replace_index < len(replaces):
                    replace_text = replaces[replace_index]
                    replace_index += 1

                if replace_text is None:
                    parent_element = element.parentNode
                    parent_element.removeChild(element)

                else:
                    font_size_style = Style(name="FontSize", family="paragraph")
                    font_size_style.addElement(TextProperties(fontsize=font_size))
                    self.doc.styles.addElement(font_size_style)

                    span = Span(stylename=font_size_style, text=replace_text)
                    new_element.addElement(span)

                    parent_element = element.parentNode
                    parent_element.insertBefore(new_element, element)
                    parent_element.removeChild(element)

    def save(self, path: str | Path) -> None:
        self.doc.save(path)


if __name__ == "__main__":
    editor = DocEditor(ROOT_DIR / "test.odt")

    editor.searchNReplace(
        "#TotalSearchYandex",
        ["\t\t- 5000 – количество поисковых релевантных запросов в системе yandex.ru"],
    )

    editor.searchNReplace(
        "#TotalSearchGoogle",
        ["\t\t- 5000 – количество поисковых релевантных запросов в системе google.com"],
    )

    editor.save(ROOT_DIR / "new_text.odt")
