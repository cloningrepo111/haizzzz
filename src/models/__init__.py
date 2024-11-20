from typing import Annotated

from pydantic import StringConstraints

StrictStr = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]
StripStr = Annotated[str, StringConstraints(strip_whitespace=True)]