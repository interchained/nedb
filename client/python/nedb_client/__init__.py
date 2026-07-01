"""
nedb-client — async Python client for the nedbd HTTP API.

Usage:
    from nedb_client import NedbClient

    async with NedbClient("http://127.0.0.1:7070", db="mydb") as db:
        await db.put("blocks", "618000", {"height": 618000, "hash": "000abc"})
        rows = await db.query("FROM blocks ORDER BY height DESC LIMIT 10")
        head = await db.head()   # BLAKE2b Merkle root
"""

from .client import NedbClient, NedbError

__version__ = "2.5.44"
__all__ = ["NedbClient", "NedbError"]
