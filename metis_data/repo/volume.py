from __future__ import annotations
from dataclasses import dataclass
import metis_data

@dataclass
class CheckpointVolume:
    ns: metis_data.NameSpace

    def __post_init__(self):
        self.ns.create_checkpoint_volume(self)



@dataclass
class DeltaCheckpoint(CheckpointVolume):
    name: str


@dataclass
class CheckpointLocal(CheckpointVolume):
    name: str
    path: str
