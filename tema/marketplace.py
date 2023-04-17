"""
This module represents the Marketplace.

Computer Systems Architecture Course
Assignment 1
March 2021
"""
from threading import Lock
import unittest
import logging
from logging.handlers import RotatingFileHandler
from time import gmtime, strftime
from os.path import isfile
from sys import exit as sysexit


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

        logging.Formatter.converter = gmtime
        self.logger = logging.getLogger("Marketplace_logger")
        self.logger.setLevel(logging.INFO)
        should_roll = isfile("marketplace.log")
        handler = RotatingFileHandler(filename="marketplace.log", maxBytes=4000000, backupCount=10)
        if should_roll:
            handler.doRollover()
        self.logger.addHandler(handler)
        self.logger.info("[%s] NEW Marketplace: %s",
                         strftime("%d.%m.%Y-%H:%M:%S", gmtime()), str(queue_size_per_producer))

    def register_producer(self):
        """
        Returns an id for the producer that calls this.
        """
        self.logger.info("[%s] IN register_producer", strftime("%d.%m.%Y-%H:%M:%S", gmtime()))
        with self.register_lock:
            producer_id = len(self.producer_queues)
            self.producer_queues.append(self.queue_size_per_producer)
            self.logger.info("[%s] OUT register_producer: %s",
                             strftime("%d.%m.%Y-%H:%M:%S", gmtime()), str(producer_id))
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
        self.logger.info("[%s] IN publish: %s, %s",
                         strftime("%d.%m.%Y-%H:%M:%S", gmtime()), str(producer_id), str(product))

        if producer_id >= len(self.producer_queues):
            self.logger.error("[%s] ERROR: Invalid producer id",
                              strftime("%d.%m.%Y-%H:%M:%S", gmtime()))
            sysexit(-1)

        if self.producer_queues[producer_id] > 0:
            self.products.append([product, -1, producer_id])
            self.producer_queues[producer_id] -= 1
            self.logger.info("[%s] OUT publish: True", strftime("%d.%m.%Y-%H:%M:%S", gmtime()))
            return True

        self.logger.info("[%s] OUT publish: False", strftime("%d.%m.%Y-%H:%M:%S", gmtime()))
        return False

    def new_cart(self):
        """
        Creates a new cart for the consumer

        :returns an int representing the cart_id
        """
        self.logger.info("[%s] IN new_cart", strftime("%d.%m.%Y-%H:%M:%S", gmtime()))
        with self.register_lock:
            self.cart_count += 1
            self.logger.info("[%s] OUT new_cart: %s",
                             strftime("%d.%m.%Y-%H:%M:%S", gmtime()), str(self.cart_count - 1))
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
        self.logger.info("[%s] IN add_to_cart: %s, %s",
                         strftime("%d.%m.%Y-%H:%M:%S", gmtime()), str(cart_id), str(product))

        if cart_id >= self.cart_count:
            self.logger.error("[%s] ERROR: Invalid cart id",
                              strftime("%d.%m.%Y-%H:%M:%S", gmtime()))
            sysexit(-1)

        with self.products_lock:
            for current_product in self.products:
                if current_product[0] == product and current_product[1] == -1:
                    current_product[1] = cart_id
                    self.logger.info("[%s] OUT add_to_cart: True",
                                     strftime("%d.%m.%Y-%H:%M:%S", gmtime()))
                    return True

            self.logger.info("[%s] OUT add_to_cart: False",
                             strftime("%d.%m.%Y-%H:%M:%S", gmtime()))
            return False

    def remove_from_cart(self, cart_id, product):
        """
        Removes a product from cart.

        :type cart_id: Int
        :param cart_id: id cart

        :type product: Product
        :param product: the product to remove from cart
        """
        self.logger.info("[%s] IN remove_from_cart: %s, %s",
                         strftime("%d.%m.%Y-%H:%M:%S", gmtime()), str(cart_id), str(product))

        if cart_id >= self.cart_count:
            self.logger.error("[%s] ERROR: Invalid cart id",
                              strftime("%d.%m.%Y-%H:%M:%S", gmtime()))
            sysexit(-1)

        with self.products_lock:
            for current_product in self.products:
                if current_product[0] == product and current_product[1] == cart_id:
                    current_product[1] = -1
                    self.logger.info("[%s] OUT remove_from_cart",
                                     strftime("%d.%m.%Y-%H:%M:%S", gmtime()))
                    return

    def place_order(self, cart_id):
        """
        Return a list with all the products in the cart.

        :type cart_id: Int
        :param cart_id: id cart
        """
        self.logger.info("[%s] IN place_order: %s",
                         strftime("%d.%m.%Y-%H:%M:%S", gmtime()), str(cart_id))

        if cart_id >= self.cart_count:
            self.logger.error("[%s] ERROR: Invalid cart id",
                              strftime("%d.%m.%Y-%H:%M:%S", gmtime()))
            sysexit(-1)

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
        self.logger.info("[%s] OUT place_order: %s",
                         strftime("%d.%m.%Y-%H:%M:%S", gmtime()), str(return_products))
        return return_products


class TestMarketplace(unittest.TestCase):
    """
    Class used to unit test the Marketplace class.
    """
    def setUp(self):
        """
        Create a new marketplace for every test.
        """
        self.marketplace = Marketplace(2)

    def test_default_values(self):
        """
        Check default values of the marketplace.
        """
        self.assertEqual(self.marketplace.queue_size_per_producer, 2,
                         "incorrect default value, correct: 2")
        self.assertEqual(len(self.marketplace.producer_queues), 0,
                         "incorrect default size, correct: 0")
        self.assertEqual(len(self.marketplace.products), 0, "incorrect default size, correct: 0")
        self.assertEqual(self.marketplace.cart_count, 0, "incorrect default value, correct: 0")

    def test_register_producer(self):
        """
        Check register producer method.
        """
        self.assertEqual(self.marketplace.register_producer(), 0, "wrong producer id, correct: 0")
        self.assertEqual(self.marketplace.register_producer(), 1, "wrong producer id, correct: 1")

    def test_publish(self):
        """
        Check publish method.
        """
        producer_id = self.marketplace.register_producer()
        products = ["Espresso", "Americano", "Mint Tea"]
        self.assertTrue(self.marketplace.publish(producer_id, products[0]),
                        "incorrect result, correct: True")
        self.assertEqual(self.marketplace.products[0], [products[0], -1, producer_id],
                         "published incorrectly")
        self.assertTrue(self.marketplace.publish(producer_id, products[1]),
                        "incorrect result, correct: True")
        self.assertFalse(self.marketplace.publish(producer_id, products[2]),
                         "incorrect result, correct: False")

    def test_new_cart(self):
        """
        Check new cart method.
        """
        self.assertEqual(self.marketplace.new_cart(), 0, "wrong producer id, correct: 0")
        self.assertEqual(self.marketplace.new_cart(), 1, "wrong producer id, correct: 1")

    def test_add_to_cart(self):
        """
        Check add to cart method.
        """
        producer_id = self.marketplace.register_producer()
        product = "Espresso"
        cart_id = self.marketplace.new_cart()
        self.assertTrue(self.marketplace.publish(producer_id, product),
                        "wrong publish result, correct: True")
        self.assertTrue(self.marketplace.add_to_cart(cart_id, product),
                        "wrong add_to_cart result, correct: True")
        self.assertFalse(self.marketplace.add_to_cart(cart_id, product),
                         "wrong add_to_cart result, correct: False")

    def test_remove_from_cart(self):
        """
        Check remove from cart method.
        """
        producer_id = self.marketplace.register_producer()
        product = "Espresso"
        cart_id = self.marketplace.new_cart()
        self.assertTrue(self.marketplace.publish(producer_id, product),
                        "wrong publish result, correct: True")
        self.assertTrue(self.marketplace.add_to_cart(cart_id, product),
                        "wrong add_to_cart result, correct: True")
        self.marketplace.remove_from_cart(cart_id, product)
        self.assertEqual(self.marketplace.products[0][1], -1, "didn't remove")

    def test_place_order(self):
        """
        Check place order method.
        """
        producer_id = self.marketplace.register_producer()
        products = ["Espresso", "Americano"]

        self.assertTrue(self.marketplace.publish(producer_id, products[0]),
                        "wrong publish result, correct: True")
        self.assertTrue(self.marketplace.publish(producer_id, products[1]),
                        "wrong publish result, correct: True")

        cart_id = self.marketplace.new_cart()
        self.assertTrue(self.marketplace.add_to_cart(cart_id, products[1]),
                        "wrong add_to_cart result, correct: True")
        self.assertEqual(self.marketplace.place_order(cart_id), [products[1]],
                         "incorrect result, correct: americano coffee")
