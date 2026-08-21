import bisect
import zlib
from typing import Dict, List


def fnv1a_32(key: str) -> int:
    """FNV-1a 32-bit hash matching Go's fnv.New32a()."""
    hash_val = 2166136261
    for char in key.encode("utf-8"):
        hash_val ^= char
        hash_val = (hash_val * 16777619) & 0xFFFFFFFF
    return hash_val


class ConsistentHashRing:
    def __init__(self, vnodes_per_node: int = 256):
        self.vnodes_per_node = vnodes_per_node
        self.ring: List[int] = []
        self.vnode_to_shard: Dict[int, str] = {}
        self.shards: Dict[str, bool] = {}

    def add_shard(self, shard_id: str):
        if shard_id in self.shards:
            return
        self.shards[shard_id] = True

        for i in range(self.vnodes_per_node):
            vnode_key = f"{shard_id}#{i}"
            h = fnv1a_32(vnode_key)
            self.ring.append(h)
            self.vnode_to_shard[h] = shard_id

        self.ring.sort()

    def remove_shard(self, shard_id: str):
        if shard_id not in self.shards:
            return
        del self.shards[shard_id]

        new_ring = []
        for h in self.ring:
            if self.vnode_to_shard.get(h) == shard_id:
                del self.vnode_to_shard[h]
            else:
                new_ring.append(h)
        self.ring = new_ring

    def get_shard(self, key: str) -> str:
        if not self.ring:
            raise ValueError("No shards configured in hash ring")

        h = fnv1a_32(key)
        idx = bisect.bisect_left(self.ring, h)
        if idx == len(self.ring):
            idx = 0
        return self.vnode_to_shard[self.ring[idx]]
