from datetime import datetime
from logging import Logger
from lib.kafka_connect import KafkaConsumer, KafkaProducer
from dds_loader.repository.dds_repository import OrderDdsBuilder, DdsRepository


class DdsMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 dds_repo: DdsRepository,
                 batch_size: int,
                 logger: Logger) -> None:

        self._consumer = consumer
        self._producer = producer
        self._dds_repo = dds_repo
        self._logger = logger
        self._batch_size = 30

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()

            if not msg:
                self._logger.info(f"{datetime.utcnow()}: There is no message")   
                break

            builder = OrderDdsBuilder(msg['payload'])
            self._logger.info(
                    f"{datetime.utcnow()}: OrderDdsBuilder created successfully")
            self._logger.info(
                    f"{datetime.utcnow()}: Starting HUB tables insertion...")

            # Хаб ресторанов
            h_restaurant = builder.h_restaurant()
            self._dds_repo.h_restaurant_insert(h_restaurant)
            self._logger.info(
                    f"{datetime.utcnow()}: ✓ H_Restaurant inserted: {h_restaurant.restaurant_id} -> {h_restaurant.h_restaurant_pk}")

            # Хаб пользователей
            h_user = builder.h_user()
            self._dds_repo.h_user_insert(h_user)
            self._logger.info(
                    f"{datetime.utcnow()}: ✓ H_User inserted: {h_user.user_id} -> {h_user.h_user_pk}")

            # Хаб заказов
            h_order = builder.h_order()
            self._dds_repo.h_order_insert(h_order)
            self._logger.info(
                    f"{datetime.utcnow()}: ✓ H_Order inserted: {h_order.order_id} -> {h_order.h_order_pk}")

            # Хабы продуктов
            h_products = builder.h_product()
            for h_product in h_products:
                self._dds_repo.h_product_insert(h_product)
            self._logger.info(
                    f"{datetime.utcnow()}: ✓ H_Product inserted: {len(h_products)} products")

            # Хабы категорий
            h_categories = builder.h_category()
            for h_category in h_categories:
                self._dds_repo.h_category_insert(h_category)
            self._logger.info(
                    f"{datetime.utcnow()}: ✓ H_Category inserted: {len(h_categories)} categories")

            self._logger.info(
                    f"{datetime.utcnow()}: ✅ All HUB tables completed successfully")

            self._logger.info(
                    f"{datetime.utcnow()}: Starting LINK tables insertion...")

            # Линк заказ-пользователь
            l_order_user = builder.l_order_user()
            self._dds_repo.l_order_user_insert(l_order_user)
            self._logger.info(
                    f"{datetime.utcnow()}: ✓ L_Order_User inserted: {l_order_user.hk_order_user_pk}")

            # Линки заказ-продукт
            l_order_products = builder.l_order_proudct()
            for l_order_product in l_order_products:
                self._dds_repo.l_order_proudct_insert(l_order_product)
            self._logger.info(
                    f"{datetime.utcnow()}: ✓ L_Order_Product inserted: {len(l_order_products)} relations")

            # Линки продукт-ресторан
            l_product_restaurants = builder.l_proudct_restaurant()
            for l_product_restaurant in l_product_restaurants:
                self._dds_repo.l_proudct_restaurant_insert(
                        l_product_restaurant)
            self._logger.info(
                    f"{datetime.utcnow()}: ✓ L_Product_Restaurant inserted: {len(l_product_restaurants)} relations")

            # Линки продукт-категория
            l_product_categories = builder.l_proudct_category()
            for l_product_category in l_product_categories:
                self._dds_repo.l_proudct_category_insert(
                        l_product_category)
            self._logger.info(
                    f"{datetime.utcnow()}: ✓ L_Product_Category inserted: {len(l_product_categories)} relations")

            self._logger.info(
                    f"{datetime.utcnow()}: ✅ All LINK tables completed successfully")

            self._logger.info(
                    f"{datetime.utcnow()}: Starting SATELLITE tables insertion...")

            # Саттелиты названий продуктов
            s_product_names = builder.s_product_names()
            for s_product_name in s_product_names:
                self._dds_repo.s_product_names_insert(s_product_name)
            self._logger.info(
                    f"{datetime.utcnow()}: ✓ S_Product_Names inserted: {len(s_product_names)} records")

            # Саттелит названия ресторана
            s_restaurant_names = builder.s_restaurant_names()
            self._dds_repo.s_restaurant_names_insert(s_restaurant_names)
            self._logger.info(
                    f"{datetime.utcnow()}: ✓ S_Restaurant_Names inserted: {s_restaurant_names.name}")

            # Саттелит имен пользователя
            s_user_names = builder.s_user_names()
            self._dds_repo.s_user_names_insert(s_user_names)
            self._logger.info(
                    f"{datetime.utcnow()}: ✓ S_User_Names inserted: {s_user_names.username} ({s_user_names.userlogin})")

            # Саттелит статуса заказа
            s_order_status = builder.s_order_status()
            self._dds_repo.s_order_status_insert(s_order_status)
            self._logger.info(
                    f"{datetime.utcnow()}: ✓ S_Order_Status inserted: {s_order_status.status}")

            # Саттелит стоимости заказа
            s_order_cost = builder.s_order_cost()
            self._dds_repo.s_order_cost_insert(s_order_cost)
            self._logger.info(
                    f"{datetime.utcnow()}: ✓ S_Order_Cost inserted: cost={s_order_cost.cost}, payment={s_order_cost.payment}")

            self._logger.info(
                    f"{datetime.utcnow()}: ✅ All SATELLITE tables completed successfully")

            self._logger.info(
                    f"{datetime.utcnow()}: Preparing output message...")

            output_message = builder.output_message()
            self._producer.produce(output_message)

            self._logger.info(
                    f"{datetime.utcnow()}: ✅ Output message sent to Kafka")
            self._logger.info(
                    f"{datetime.utcnow()}:   - Topic: {self._producer.topic}")
            self._logger.info(
                    f"{datetime.utcnow()}:   - Order ID: {output_message.get('object_id', 'unknown')}")
            self._logger.info(
                    f"{datetime.utcnow()}:   - User ID: {output_message.get('payload', {}).get('user_id', 'unknown')}")
            self._logger.info(
                    f"{datetime.utcnow()}:   - Products count: {len(output_message.get('payload', {}).get('products', []))}")

        self._logger.info(f"{datetime.utcnow()}: FINISH")