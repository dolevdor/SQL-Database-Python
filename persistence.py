import atexit
import sqlite3
import sys

# Data Transfer Objects:
class Hat:
    def __init__(self, id, topping, supplier, quantity):
        self.id = id
        self.topping = topping
        self.supplier = supplier
        self.quantity = quantity


class Supplier:
    def __init__(self, id, name):
        self.id = id
        self.name = name


class Order:
    def __init__(self, id, location, hat):
        self.id = id
        self.location = location
        self.hat = hat


# Data Access Objects:
# All of these are meant to be singletons
class _Hats:
    def __init__(self, conn):
        self._conn = conn

    def insert(self, hat):
        self._conn.execute("""
               INSERT INTO hats (id, topping, supplier, quantity) VALUES (?, ?, ?, ?)
           """, [hat.id, hat.topping, hat.supplier, hat.quantity])

    def find(self, id):
        c = self._conn.cursor()
        c.execute("""
            SELECT * FROM hats WHERE id = ?
        """, [id])

        return Hat(*c.fetchone())

    def getByTopping(self, topping):
        c = self._conn.cursor()
        c.execute("""
            SELECT * FROM hats WHERE topping = ?
            ORDER BY id ASC
        """, [topping])

        hats_list = c.fetchall()
        sorted_by_supplier = sorted(hats_list, key=lambda tup: tup[2])
        hat = Hat(*sorted_by_supplier[0])
        hat.quantity = hat.quantity-1

        c.execute("""
            UPDATE hats
            SET quantity = (?)
            WHERE id = (?)
        """, [hat.quantity, hat.id])

        if hat.quantity == 0:
            c.execute("""
                DELETE FROM hats WHERE ID = ?
            """, [hat.id])

        return hat


class _Suppliers:
    def __init__(self, conn):
        self._conn = conn

    def insert(self, supplier):
        self._conn.execute("""
                INSERT INTO suppliers (id, name) VALUES (?, ?)
        """, [supplier.id, supplier.name])

    def find(self, id):
        c = self._conn.cursor()
        c.execute("""
            SELECT * FROM suppliers WHERE id = ?
        """, [id])

        return Supplier(*c.fetchone())


class _Orders:
    def __init__(self, conn):
        self._conn = conn

    def insert(self, order):
        self._conn.execute("""
            INSERT INTO orders (id, location, hat) VALUES (?, ?, ?)
        """, [order.id, order.location, order.hat])

    def find(self, id):
        c = self._conn.cursor()
        c.execute("""
                SELECT hat FROM orders WHERE id = ?
            """, [id])

        return Hat(*c.fetchone())


class _Repository:
    def __init__(self, dbpath):
        self._conn = sqlite3.connect(dbpath)
        self.hats = _Hats(self._conn)
        self.suppliers = _Suppliers(self._conn)
        self.orders = _Orders(self._conn)

    def _close(self):
        self._conn.commit()
        self._conn.close()

    def create_tables(self):
        self._conn.executescript("""
        CREATE TABLE if not exists hats (
            id          INTEGER     PRIMARY KEY,
            topping     STRING      NOT NULL,
            supplier    INTEGER,
            quantity    INTEGER     NOT NULL,
        
 
            FOREIGN KEY (supplier)  REFERENCES suppliers(id)
        );

        CREATE TABLE if not exists suppliers (
            id          INTEGER     PRIMARY KEY,
            name        STRING      NOT NULL
        );

        CREATE TABLE if not exists orders (
            id          INTEGER        PRIMARY KEY,
            location    STRING         NOT NULL,
            hat         INTEGER,

            FOREIGN KEY (hat)  REFERENCES hats(id)
        );
    """)
