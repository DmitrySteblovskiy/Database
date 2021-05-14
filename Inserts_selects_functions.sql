INSERT
INTO products (factory_code, name, manufact_code, price, Relevance)
VALUES (49801, 'MSI GE72MVR Apache Pro', 1, 79900, 1);

SELECT * FROM products;

INSERT
INTO countries (name, development_level, local_manufacturing, customs_duty)
VALUES ('Russia','medium', true, 15);

SELECT * FROM countries;

INSERT
INTO manufacturer (company_name, world_rank, reputation)
VALUES ('MSI', 2, 2);

SELECT * FROM manufacturer;

INSERT
INTO factors (name, covid_influence, bitcoin_rate_impact)
VALUES ('Russia', 15, 30);

SELECT * FROM factors;

INSERT
INTO prices (name, vat, economic_forecast)
VALUES ('Russia', 20, 'B-');

--SELECT * FROM prices;

SELECT id, company_name, world_rank  --Благодаря этому запросу пользователь увидит отсортированный список компаний с
FROM manufacturer          --наиболее высоким рейтингом по среднему уровню качества
ORDER BY world_rank DESC;


SELECT id, name, relevance  --Этот запрос поможет пользователю определиться с выбором ноутбука по его релевантности
FROM products               --в желаемой пользователем ценовой категории
WHERE relevance > 1
GROUP BY id, name, relevance
HAVING max(price) < 80000;


CREATE FUNCTION count_price_with_vat(x double precision, y double precision) --Функция для учета НДС при подсчете цены
RETURNS double precision AS $$
SELECT x * (1 + 1.0 * y/100);
$$ LANGUAGE SQL;

SELECT count_price_with_vat(50, 10) AS answer;

SELECT id, name, price, relevance, sum(relevance)   --Запрос сортирует по релевантности и разбивает по номерам компаний,
OVER (PARTITION BY id ORDER BY relevance DESC) AS good_choice  -- что наглядно показывает, у какой компании в среднем
FROM products;                                                 -- более релевантная продукция

SELECT id, name, development_level, customs_duty, sum(customs_duty)   --Здесь можно заметить связь между уровнем
OVER (PARTITION BY development_level ORDER BY customs_duty DESC) AS taxes_level  --развития страны и уровнем пошлин
FROM countries;