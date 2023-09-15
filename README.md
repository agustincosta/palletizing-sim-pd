# palletizing-sim-pd
Numeric simulation of multi-input and multi-output robotic palletizing system implemented in pandas.

The simulated system consists of a single palletizing robot that has as inputs complete, single SKU pallets coming from a factory, comprised of 15 layers that are moved one by one into exit pallets meant for distribution and sales centers. As each center doesn't sell considerable amounts of each SKU, sending the complete pallets as they are manufactured isn't a posibility. Instead, pallets comprised of many layers of different products are put together to fulfill each destinations' needs.

The output is dictated by the days' orders, which are the different amounts of layers of each SKU required by each destination for each day. The algorithm procedurally assigns the input layers to exit pallets to optimize the robot movements and fulfill the days' orders.

The algorithm calculates starting statistics for all the orders in .csv file and simulates the palletizing process for each day, returning relevant statistics like total movements and maximum number of concurrent pallets.

The simulation process consists of starting with a predefined amount of input pallets (parameter for the algorithm). The amount of exit pallets is not limited, each time the system cannot assign a layer to the existing incomplete exit pallets, it creates a new position. Each time an exit pallet is completed, either because of the maximum amount of layers is reached, or there are no more layers to be assigned to the given destination, the pallet is removed from the exit list.

This allows to specify a maximum of physical positions required for a robotic system such as the one described for a representative sample of production.
