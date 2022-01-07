#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import abc
from typing import overload
from typing import Any, Callable, Generic, List, Optional
from pyspark.ml._typing import P, T
import pyspark.ml._typing

import pyspark.ml.util
from pyspark.ml.linalg import DenseVector, Matrix

class Param(Generic[T]):
    parent: str
    name: str
    doc: str
    typeConverter: Callable[[Any], T]
    def __init__(
        self,
        parent: pyspark.ml.util.Identifiable,
        name: str,
        doc: str,
        typeConverter: Optional[Callable[[Any], T]] = ...,
    ) -> None: ...
    def __hash__(self) -> int: ...
    def __eq__(self, other: Any) -> bool: ...

class TypeConverters:
    @staticmethod
    def identity(value: T) -> T: ...
    @staticmethod
    def toList(value: Any) -> List: ...
    @staticmethod
    def toListFloat(value: Any) -> List[float]: ...
    @staticmethod
    def toListInt(value: Any) -> List[int]: ...
    @staticmethod
    def toListString(value: Any) -> List[str]: ...
    @staticmethod
    def toVector(value: Any) -> DenseVector: ...
    @staticmethod
    def toMatrix(value: Any) -> Matrix: ...
    @staticmethod
    def toFloat(value: Any) -> float: ...
    @staticmethod
    def toInt(value: Any) -> int: ...
    @staticmethod
    def toString(value: Any) -> str: ...
    @staticmethod
    def toBoolean(value: Any) -> bool: ...

class Params(pyspark.ml.util.Identifiable, metaclass=abc.ABCMeta):
    def __init__(self) -> None: ...
    @property
    def params(self) -> List[Param]: ...
    def explainParam(self, param: str) -> str: ...
    def explainParams(self) -> str: ...
    def getParam(self, paramName: str) -> Param: ...
    @overload
    def isSet(self, param: str) -> bool: ...
    @overload
    def isSet(self, param: Param[Any]) -> bool: ...
    @overload
    def hasDefault(self, param: str) -> bool: ...
    @overload
    def hasDefault(self, param: Param[Any]) -> bool: ...
    @overload
    def isDefined(self, param: str) -> bool: ...
    @overload
    def isDefined(self, param: Param[Any]) -> bool: ...
    def hasParam(self, paramName: str) -> bool: ...
    @overload
    def getOrDefault(self, param: str) -> Any: ...
    @overload
    def getOrDefault(self, param: Param[T]) -> T: ...
    def extractParamMap(
        self, extra: Optional[pyspark.ml._typing.ParamMap] = ...
    ) -> pyspark.ml._typing.ParamMap: ...
    def copy(self, extra: Optional[pyspark.ml._typing.ParamMap] = ...) -> Params: ...
    def set(self, param: Param, value: Any) -> None: ...
    def clear(self, param: Param) -> None: ...
    def _setDefault(self: P, **kwargs: Any) -> P: ...
    @staticmethod
    def _dummy() -> Params: ...
