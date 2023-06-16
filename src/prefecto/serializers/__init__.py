"""
Extendeds the `Serializer` class for Prefect. Allows for custom serializers to be
registered with their own methods for reading and writing. Good for complex
types with standard read and write methods such as DataFrames.
"""
from .core import ExtendedSerializer, Method, get_method
