from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Deque, Iterable, List, Optional, Tuple

from textual.containers import Horizontal
from textual.widget import Widget
from textual.widgets import Button, DataTable, Label


@dataclass
class Page:
    columns: List[str]
    rows: List[List[str]]


class PagedResultView(Widget):
    def __init__(self) -> None:
        super().__init__()
        self.table = DataTable()
        self.status = Label("No data")
        self.prev_button = Button("Prev", id="prev")
        self.next_button = Button("Next", id="next")
        self._pages: Deque[Page] = deque(maxlen=5)
        self._page_index = 0
        self._page_iter: Optional[Iterable[Tuple[List[str], List[List[str]]]]] = None
        self._total_loaded = 0

    def compose(self):
        yield Horizontal(self.prev_button, self.next_button, self.status)
        yield self.table

    def set_iterator(self, iterator: Iterable[Tuple[List[str], List[List[str]]]]) -> None:
        self._pages.clear()
        self._page_iter = iterator
        self._page_index = 0
        self._total_loaded = 0
        self.table.clear(columns=True)
        self.status.update("Ready")

    def show_page(self, page: Page, index: int) -> None:
        self.table.clear(columns=True)
        if page.columns:
            self.table.add_columns(*page.columns)
        for row in page.rows:
            self.table.add_row(*[str(value) for value in row])
        self.status.update(f"Page {index + 1} (loaded {self._total_loaded} rows)")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "next":
            self.load_next()
        elif event.button.id == "prev":
            self.load_prev()

    def load_next(self) -> None:
        if self._page_iter is None:
            return
        if self._page_index < len(self._pages) - 1:
            self._page_index += 1
            self.show_page(self._pages[self._page_index], self._page_index)
            return
        try:
            columns, rows = next(self._page_iter)
        except StopIteration:
            self.status.update("No more pages")
            return
        page = Page(columns=columns, rows=rows)
        self._pages.append(page)
        self._page_index = len(self._pages) - 1
        self._total_loaded += len(rows)
        self.show_page(page, self._page_index)

    def load_prev(self) -> None:
        if self._page_index <= 0:
            self.status.update("No previous page")
            return
        self._page_index -= 1
        self.show_page(self._pages[self._page_index], self._page_index)
