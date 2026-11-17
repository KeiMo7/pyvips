import logging
from os import PathLike
from typing import Any, Dict, List, Optional, Union

from pyvips.error import Error
from pyvips.vimage import Image

logger: logging.Logger

BIT_DEPTH_8: int
BIT_DEPTH_10: int
DITHER_NONE: str
DITHER_ORDERED: str
PIXEL_FORMATS: Dict[str, Dict[str, Any]]

class BitDepthError(Error): ...

def detect_bit_depth(image: Image,
                     declared: Optional[int] = None) -> int: ...
def convert_bit_depth(image: Image, target: int,
                      dither: str = ...) -> Image: ...
def convert_pixel_format(image: Image, pixel_format: str) -> Image: ...
def process_image(source: Union[str, PathLike],
                  target: Union[str, PathLike], *,
                  bit_depth: Optional[int] = ...,
                  dither: str = ...,
                  pixel_format: Optional[str] = ...,
                  declared_bit_depth: Optional[int] = ...,
                  **save_options: Any) -> int: ...
def process_batch(sources: List[Union[str, PathLike]],
                  targets: List[Union[str, PathLike]],
                  **options: Any) -> List[int]: ...
