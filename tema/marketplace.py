"""
This module represents the Marketplace.

Computer Systems Architecture Course
Assignment 1
March 2021
"""
from threading import Lock


class Marketplace:
    """
    Class that represents the Marketplace. It's the central part of the implementation.
    The producers and consumers use its methods concurrently.
    """
    def __init__(self, queue_size_per_producer):
        """
        Constructor

        :type queue_size_per_producer: Int
        :param queue_size_per_producer: the maximum size of a queue associated with each producer
        """
        self.queue_size_per_producer = queue_size_per_producer
        self.producer_queues = []
        self.products = []
        self.cart_count = 0
        self.products_lock = Lock()
        self.register_lock = Lock()
        self.print_lock = Lock()

    def register_producer(self):
        """
        Returns an id for the producer that calls this.
        """
        with self.register_lock:
            producer_id = len(self.producer_queues)
            self.producer_queues.append(self.queue_size_per_producer)
            return producer_id

    def publish(self, producer_id, product):
        """
        Adds the product provided by the producer to the marketplace

        :type producer_id: String
        :param producer_id: producer id

        :type product: Product
        :param product: the Product that will be published in the Marketplace

        :returns True or False. If the caller receives False, it should wait and then try again.
        """
        if self.producer_queues[producer_id] > 0:
            self.products.append([product, -1, producer_id])
            self.producer_queues[producer_id] -= 1
            return True
        return False

    def new_cart(self):
        """
        Creates a new cart for the consumer

        :returns an int representing the cart_id
        """
        with self.register_lock:
            self.cart_count += 1
            return self.cart_count - 1

    def add_to_cart(self, cart_id, product):
        """
        Adds a product to the given cart. The method returns

        :type cart_id: Int
        :param cart_id: id cart

        :type product: Product
        :param product: the product to add to cart

        :returns True or False. If the caller receives False, it should wait and then try again
        """
        with self.products_lock:
            for current_product in self.products:
                if current_product[0] == product and current_product[1] == -1:
                    current_product[1] = cart_id
                    return True
            return False

    def remove_from_cart(self, cart_id, product):
        """
        Removes a product from cart.

        :type cart_id: Int
        :param cart_id: id cart

        :type product: Product
        :param product: the product to remove from cart
        """
        with self.products_lock:
            for current_product in self.products:
                if current_product[0] == product and current_product[1] == cart_id:
                    current_product[1] = -1
                    return

    def place_order(self, cart_id):
        """
        Return a list with all the products in the cart.

        :type cart_id: Int
        :param cart_id: id cart
        """
        return_products = []
        with self.products_lock:
            initial_len = len(self.products)
            removed_products = 0
            for i in range(initial_len):
                current_product = self.products[i - removed_products]
                if current_product[1] == cart_id:
                    self.producer_queues[current_product[2]] += 1
                    return_products.append(current_product[0])
                    self.products.remove(current_product)
                    removed_products += 1
        return return_products
