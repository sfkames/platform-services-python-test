#!/usr/bin/env python
import logging

import tornado.httpserver
import tornado.ioloop
import tornado.web

from tornado.options import options
from tornado.queues import Queue

from settings import settings
from url_patterns import url_patterns

from pymongo import MongoClient

# Endpoint 1 - Calculate and store customer rewards data in DB
class CalculateRewards(tornado.web.RequestHandler):
    def post(self):
        try:
            email=self.get_argument("email")
            order_ttl="{:.2f}".format(float(self.get_argument("order_total")))
            
            # 1 pt per dollar spent
            reward_pts=int(float(order_ttl))
            
            # Calculate tier based on pts
            curr_tier=self.calculate_current_tier(reward_pts)

            # Calculate reward name based on pts
            curr_name=self.calculate_reward_name(reward_pts)

            next_tier=self.calculate_current_tier(reward_pts+100)

            next_name=self.calculate_reward_name(reward_pts+100)

            progress=calculate_progress(reward_pts)

            # Store in DB
            self.store_rewards_info(email, reward_pts, curr_tier, curr_name, next_tier, next_name, progress)

            # Response
            response={
                "email":email,
                "rewardPoints":reward_pts,
                "currentTier":curr_tier,
                "currentTierName":curr_name,
                "nextTier":next_tier,
                "nextTierName":next_name,
                "nextTierProgress":progress
            }
            self.write({"status":"success", "data":response})
        except Exception as e:
            self.set_status(400)
            self.write({"status":"error","message":str(e)})

    # Function to calculate current tier
    def calculate_current_tier(reward_pts):
        client = MongoClient("mongodb", 27017)
        db = client["Rewards"]

        rewards_collection=db["rewards"]
        reward=rewards_collection.find_one({"points": {"$lte":reward_pts}})

        tier_name=reward["tier"] if reward else None
        
        return tier_name

    # Function to calculate reward name
    def calculate_reward_name(reward_pts):
        client = MongoClient("mongodb", 27017)
        db = client["Rewards"]

        rewards_collection=db["rewards"]
        reward=rewards_collection.find_one({"points": {"$lte":reward_pts}})

        reward_name=reward["rewardName"] if reward else None
        
        return reward_name

    def store_rewards_info(email, reward_pts, curr_tier, curr_name, next_tier, next_name, progress):
        client = MongoClient("mongodb", 27017)
        db = client["Rewards"]
        collection=db["customerRewards"]

        data={
            "email":email,
            "rewardPoints":reward_pts,
            "currentTier":curr_tier,
            "currentTierName":curr_name,
            "nextTier":next_tier,
            "nextTierName":next_name,
            "nextTierProgress":progress
        }
        collection.insert(data)

    def calculate_progress(curr_pts):
        client = MongoClient("mongodb", 27017)
        db = client["Rewards"]
        collection=db["rewards"]

        tiers=collection.find().sort("points",1)
        next_tier=float('inf')

        for tier in tiers:
            if curr_pts<tier["points"]:
                next_tier=tier["points"]
                break
        
        if next_tier==float('inf'):
            return 1
        
        progress="{:.2f}".format(curr_pts/next_tier)
        return progress


# Endpoint 2 - Accept customer email, return rewards data
class SingleCustomerData(tornado.web.RequestHandler):
    def get(self):
        try:
            email=self.get_argument("email")

            customer_info=self.get_customer_info(email)

            if customer_info:
                self.write({"status": "success", "data": customer_info})
            else:
                self.set_status(404)
                self.write({"status": "error", "message": "Customer does not exist"})
        except Exception as e:
            self.set_status(400)
            self.write({"status":"error","message":str(e)})

    # Function to retrieve customer info
    def get_customer_info(self, email):
        client = MongoClient("mongodb", 27017)
        db = client["Rewards"]
        collection=db["customerRewards"]

        customer_info=collection.find_one({"email": email})
        return customer_info
    

# Endpoint 3 - Return all customer rewards data
class AllCustomerData(tornado.web.RequestHandler):
    def get(self):
        try:
            all_customer_info=self.get_all_info()
            self.write({"status": "success", "data":all_customer_info})

        except Exception as e:
            self.set_status(400)
            self.write({"status":"error","message": str(e)})

    def get_all_info(self):
        client = MongoClient("mongodb", 27017)
        db = client["Rewards"]
        collection=db["customerRewards"]

        all_customer_info=list(collection.find())
        return all_customer_info


class App(tornado.web.Application):
    def __init__(self, urls):
        self.logger = logging.getLogger(self.__class__.__name__)

        tornado.web.Application.__init__(self, urls, **settings)

app = App(url_patterns)


def main():
    logger = logging.getLogger()
    tornado.options.parse_command_line()
    http_server = tornado.httpserver.HTTPServer(app, xheaders=True)
    http_server.listen(options.port)

    logger.info('Tornado server started on port {}'.format(options.port))

    try:
        tornado.ioloop.IOLoop.instance().start()
    except KeyboardInterrupt:
        logger.info("\nStopping server on port {}".format(options.port))


if __name__ == "__main__":
    main()
