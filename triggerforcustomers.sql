INSERT INTO customers (customer_code, completed_orders, total_orders, lastcompleted_order, last_order, minamount, maxamount, avgamount)
SELECT
    customer_code,
    SUM(CASE WHEN status = 1 THEN 1 ELSE 0 END) AS completed_orders,
    COUNT(*) AS total_orders,
    MAX(CASE WHEN status = 1 THEN updated_at ELSE NULL END) AS lastcompleted_order,
    MAX(created_at) AS last_order,
    MIN(CASE WHEN status = 1 THEN payment_amount ELSE NULL END) AS minamount,
    MAX(CASE WHEN status = 1 THEN payment_amount ELSE NULL END) AS maxamount,
    AVG(CASE WHEN status = 1 THEN payment_amount ELSE NULL END) AS avgamount
FROM orderstable
GROUP BY customer_code;


DELIMITER //

CREATE TRIGGER `orderstable_AFTER_INSERT` AFTER INSERT ON `orderstable` FOR EACH ROW
BEGIN
    INSERT INTO customers (customer_code, completed_orders, total_orders, lastcompleted_order, last_order, minamount, maxamount, avgamount)
    SELECT
        NEW.customer_code,
        SUM(CASE WHEN status = 1 THEN 1 ELSE 0 END) AS completed_orders,
        COUNT(*) AS total_orders,
        MAX(CASE WHEN status = 1 THEN updated_at ELSE NULL END) AS lastcompleted_order,
        MAX(created_at) AS last_order,
        MIN(CASE WHEN status = 1 THEN payment_amount ELSE NULL END) AS minamount,
        MAX(CASE WHEN status = 1 THEN payment_amount ELSE NULL END) AS maxamount,
        AVG(CASE WHEN status = 1 THEN payment_amount ELSE NULL END) AS avgamount
    FROM orderstable
    WHERE customer_code = NEW.customer_code
    ON DUPLICATE KEY UPDATE
        completed_orders = VALUES(completed_orders),
        total_orders = VALUES(total_orders),
        lastcompleted_order = VALUES(lastcompleted_order),
        last_order = VALUES(last_order),
        minamount = VALUES(minamount),
        maxamount = VALUES(maxamount),
        avgamount = VALUES(avgamount);
END //
DELIMITER ;


DELIMITER //
CREATE TRIGGER `orderstable_AFTER_UPDATE` AFTER UPDATE ON `orderstable` FOR EACH ROW
BEGIN
    UPDATE customers
    SET
        total_orders = (SELECT COUNT(*) FROM orderstable WHERE customer_code = NEW.customer_code),
        last_order = (SELECT MAX(created_at) FROM orderstable WHERE customer_code = NEW.customer_code),
        completed_orders = (SELECT COUNT(*) FROM orderstable WHERE customer_code = NEW.customer_code AND status = 1),
        lastcompleted_order = (SELECT MAX(updated_at) FROM orderstable WHERE customer_code = NEW.customer_code AND status = 1),
        minamount = (SELECT MIN(payment_amount) FROM orderstable WHERE customer_code = NEW.customer_code AND status = 1),
        maxamount = (SELECT MAX(payment_amount) FROM orderstable WHERE customer_code = NEW.customer_code AND status = 1),
        avgamount = (SELECT AVG(payment_amount) FROM orderstable WHERE customer_code = NEW.customer_code AND status = 1)
    WHERE customer_code = NEW.customer_code;
END //


DELIMITER ;
