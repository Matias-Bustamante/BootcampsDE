

select 
c.category_name as name, 
p.product_name as product_name, 
p.unit_price as unite_price, 
AVG(p.unit_price) over(partition by c.category_id) as avg_unit_price
from categories c inner join products p 
on c.category_id =p.category_id
