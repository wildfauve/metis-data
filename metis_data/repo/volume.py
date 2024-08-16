from __future__ import annotations
from dataclasses import dataclass
import metis_data


@dataclass
class CheckpointVolumeRoot:
    name: str


@dataclass
class CheckpointVolumeWithPath(CheckpointVolumeRoot):
    path: str

