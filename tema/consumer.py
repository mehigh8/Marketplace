"""
This module represents the Consumer.

Computer Systems Architecture Course
Assignment 1
March 2021
"""

from threading import Thread
from time import sleep


class Consumer(Thread):
    """
    Class that represents a consumer.
    """

    def __init__(self, carts, marketplace, retry_wait_time, **kwargs):
        """
        Constructor.

        :type carts: List
        :param carts: a list of add and remove operations

        :type marketplace: Marketplace
        :param marketplace: a reference to the marketplace

        :type retry_wait_time: Time
        :param retry_wait_time: the number of seconds that a producer must wait
        until the Marketplace becomes available

        :type kwargs:
        :param kwargs: other arguments that are passed to the Thread's __init__()
        """
        Thread.__init__(self, **kwargs)
        self.carts = carts
        self.marketplace = marketplace
        self.retry_wait_time = retry_wait_time
        self.name = kwargs.get("name")

    def run(self):
        for cart in self.carts:
            # For every cart, the consumer gets a new id.
            cart_id = self.marketplace.new_cart()
            # Then, he goes through the operations of add/remove.
            for operation in cart:
                quantity = operation["quantity"]
                while quantity > 0:
                    if operation["type"] == "add":
                        # If the user can't add, he has to wait some time and try again.
                        while not self.marketplace.add_to_cart(cart_id, operation["product"]):
                            sleep(self.retry_wait_time)
                    else:
                        self.marketplace.remove_from_cart(cart_id, operation["product"])
                    quantity -= 1
            # After all the operations of the cart, the consumer places an order.
            products = self.marketplace.place_order(cart_id)
            # We have to print the consumer's order.
            for product in products:
                # We use a lock to ensure that the prints don't mess with each other.
                with self.marketplace.print_lock:
                    print(self.name + " bought " + str(product))
