from dataclasses import dataclass


@dataclass
class BaseValue:
    def replace(self, key, value):
        setattr(self, key, value)
        return self
