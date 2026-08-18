from typing import Iterable, Iterator, TypeVar, Sequence
from itertools import islice


T = TypeVar("T")

def batched(iterable: Iterable[T], n: int) -> Iterator[list[T]]:
    it = iter(iterable)
    while chunk := list(islice(it, n)):
        yield chunk