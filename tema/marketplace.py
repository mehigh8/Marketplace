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

        # Preparing the logger.
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
        # We use a lock to ensure that two producers can't get the same id.
        with self.register_lock:
            # We get the new id for the producer by counting how many queues we have.
            producer_id = len(self.producer_queues)
            # We create a new queue and return the id.
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
        # We check if the id is valid.
        if producer_id >= len(self.producer_queues) or producer_id < 0:
            self.logger.error("[%s] ERROR: Invalid producer id",
                              strftime("%d.%m.%Y-%H:%M:%S", gmtime()))
            sysexit(-1)

        # We check if the current producer has space in queue to publish a new product.
        if self.producer_queues[producer_id] > 0:
            # If he has, we add the product to the products buffer and change his queue value.
            self.products.append([product, -1, producer_id])
            self.producer_queues[producer_id] -= 1
            self.logger.info("[%s] OUT publish: True", strftime("%d.%m.%Y-%H:%M:%S", gmtime()))
            return True

        # If not, he has to wait and try again.
        self.logger.info("[%s] OUT publish: False", strftime("%d.%m.%Y-%H:%M:%S", gmtime()))
        return False

    def new_cart(self):
        """
        Creates a new cart for the consumer

        :returns an int representing the cart_id
        """
        self.logger.info("[%s] IN new_cart", strftime("%d.%m.%Y-%H:%M:%S", gmtime()))
        # We use a lock to ensure that two consumers don't get the same cart id.
        with self.register_lock:
            # We increment the number of carts.
            self.cart_count += 1
            self.logger.info("[%s] OUT new_cart: %s",
                             strftime("%d.%m.%Y-%H:%M:%S", gmtime()), str(self.cart_count - 1))
            # The cart id is the number of carts prior to the incrementation.
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

        # We check if the cart id is valid.
        if cart_id >= self.cart_count or cart_id < 0:
            self.logger.error("[%s] ERROR: Invalid cart id",
                              strftime("%d.%m.%Y-%H:%M:%S", gmtime()))
            sysexit(-1)

        # We use a lock to ensure that there can't be two consumers
        # adding the same product to their cart simultaneously.
        with self.products_lock:
            for current_product in self.products:
                # If we find the searched product, and it doesn't belong to another cart,
                # we can add it to the current cart.
                if current_product[0] == product and current_product[1] == -1:
                    current_product[1] = cart_id
                    self.logger.info("[%s] OUT add_to_cart: True",
                                     strftime("%d.%m.%Y-%H:%M:%S", gmtime()))
                    return True

            # If we don't find the searched product, it means the consumer has to wait
            # and try again.
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

        # We check if the cart id is valid.
        if cart_id >= self.cart_count or cart_id < 0:
            self.logger.error("[%s] ERROR: Invalid cart id",
                              strftime("%d.%m.%Y-%H:%M:%S", gmtime()))
            sysexit(-1)

        # We use a lock to ensure that if a consumer places an order and changes the buffer,
        # our search doesn't miss a product.
        with self.products_lock:
            for current_product in self.products:
                # If we find the searched product, and it belongs to our cart,
                # we mark it as unowned.
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

        # We check if the cart id is valid.
        if cart_id >= self.cart_count or cart_id < 0:
            self.logger.error("[%s] ERROR: Invalid cart id",
                              strftime("%d.%m.%Y-%H:%M:%S", gmtime()))
            sysexit(-1)

        return_products = []
        # We use a lock to ensure that while changing the buffer a consumer can add/remove
        # from their cart without the possibility to miss products.
        with self.products_lock:
            initial_len = len(self.products)
            # We store how many products we removed to iterate through the buffer correctly.
            removed_products = 0
            for i in range(initial_len):
                current_product = self.products[i - removed_products]
                # If a product is in out cart, we remove it from the buffer and store it in a
                # temporary list which will be returned, and we change the product's producer's
                # queue so he can pubish more products.
                if current_product[1] == cart_id:
                    self.producer_queues[current_product[2]] += 1
                    return_products.append(current_product[0])
                    self.products.remove(current_product)
                    removed_products += 1
        self.logger.info("[%s] OUT place_order: %s",
                         strftime("%d.%m.%Y-%H:%M:%S", gmtime()), str(return_products))
        # Finally, we return our temporary list of removed products.
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
        Check the default values of every field from the marketplace.
        """
        self.assertEqual(self.marketplace.queue_size_per_producer, 2,
                         "incorrect default value, correct: 2")
        self.assertEqual(len(self.marketplace.producer_queues), 0,
                         "incorrect default size, correct: 0")
        self.assertEqual(len(self.marketplace.products), 0, "incorrect default size, correct: 0")
        self.assertEqual(self.marketplace.cart_count, 0, "incorrect default value, correct: 0")

    def test_register_producer(self):
        """
        Check the ids returned when registering a new producer.
        """
        self.assertEqual(self.marketplace.register_producer(), 0, "wrong producer id, correct: 0")
        self.assertEqual(self.marketplace.register_producer(), 1, "wrong producer id, correct: 1")

    def test_publish(self):
        """
        Check if the publish method allows a producer to add products in the buffer when he
        has space in queue, and when he doesn't have, respectively.
        """
        producer_id = self.marketplace.register_producer()
        products = ["Espresso", "Americano", "Mint Tea"]
        self.assertTrue(self.marketplace.publish(producer_id, products[0]),
                        "incorrect result, correct: True")
        # We also check if the product was added correctly.
        self.assertEqual(self.marketplace.products[0], [products[0], -1, producer_id],
                         "published incorrectly")
        self.assertTrue(self.marketplace.publish(producer_id, products[1]),
                        "incorrect result, correct: True")
        self.assertFalse(self.marketplace.publish(producer_id, products[2]),
                         "incorrect result, correct: False")

    def test_new_cart(self):
        """
        Check the ids returned when a consumer asks for a new cart.
        """
        self.assertEqual(self.marketplace.new_cart(), 0, "wrong producer id, correct: 0")
        self.assertEqual(self.marketplace.new_cart(), 1, "wrong producer id, correct: 1")

    def test_add_to_cart(self):
        """
        Check if the add to cart method returns the correct value when the buffer has
        the searched item, and when it doesn't, respectively.
        """
        producer_id = self.marketplace.register_producer()
        product = "Espresso"
        cart_id = self.marketplace.new_cart()
        # We also check if the publish was successful.
        self.assertTrue(self.marketplace.publish(producer_id, product),
                        "wrong publish result, correct: True")
        self.assertTrue(self.marketplace.add_to_cart(cart_id, product),
                        "wrong add_to_cart result, correct: True")
        self.assertFalse(self.marketplace.add_to_cart(cart_id, product),
                         "wrong add_to_cart result, correct: False")

    def test_remove_from_cart(self):
        """
        Check if the remove from cart method actually removes the product from the cart
        by changing the ownership of said product to unowned.
        """
        producer_id = self.marketplace.register_producer()
        product = "Espresso"
        cart_id = self.marketplace.new_cart()
        # We also check if the publish and the add to cart were successful.
        self.assertTrue(self.marketplace.publish(producer_id, product),
                        "wrong publish result, correct: True")
        self.assertTrue(self.marketplace.add_to_cart(cart_id, product),
                        "wrong add_to_cart result, correct: True")
        self.marketplace.remove_from_cart(cart_id, product)
        self.assertEqual(self.marketplace.products[0][1], -1, "didn't remove")

    def test_place_order(self):
        """
        Check if the consumer gets the correct items when placing an order.
        """
        producer_id = self.marketplace.register_producer()
        products = ["Espresso", "Americano"]

        # We also check if the publishes were successful.
        self.assertTrue(self.marketplace.publish(producer_id, products[0]),
                        "wrong publish result, correct: True")
        self.assertTrue(self.marketplace.publish(producer_id, products[1]),
                        "wrong publish result, correct: True")

        cart_id = self.marketplace.new_cart()
        # We add a product to cart and place the order.
        self.assertTrue(self.marketplace.add_to_cart(cart_id, products[1]),
                        "wrong add_to_cart result, correct: True")
        self.assertEqual(self.marketplace.place_order(cart_id), [products[1]],
                         "incorrect result, correct: americano coffee")
