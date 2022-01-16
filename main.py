import sqlite3
import sys
import atexit
from persistence import _Repository, Hat, Supplier, Order


def main():
    repo = _Repository(sys.argv[4])
    atexit.register(repo._close)
    repo.create_tables()

    with open(sys.argv[1]) as inputFile:
        list = inputFile.readline().split(",")
        hats = list[0]
        suppliers = int(list[1])

        for i in range(int(hats)):
            list2 = inputFile.readline().split(",")  # to check
            hat = Hat(int(list2[0]), list2[1], int(list2[2]), int(list2[3]))
            repo.hats.insert(hat)
        for i in range(int(suppliers)):
            list3 = inputFile.readline().split(",")
            supplier = Supplier(int(list3[0]), list3[1].rstrip())
            repo.suppliers.insert(supplier)

    output_summary = []
    with open(sys.argv[2]) as ordersFile:
        lines = ordersFile.readlines()
        counter = 1

        for line in lines:
            line = line.split(",")
            location = line[0]
            topping = str(line[1]).rstrip()
            hat = repo.hats.getByTopping(topping)
            order = Order(counter, location, hat.id)
            repo.orders.insert(order)
            supplier = repo.suppliers.find(hat.supplier)
            output_summary.append(topping + "," + supplier.name.rstrip() + "," + location)
            counter = counter + 1

    with open(sys.argv[3], "w") as outputFile:
        for line in output_summary:
            outputFile.write(line)
            outputFile.write('\n')


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
