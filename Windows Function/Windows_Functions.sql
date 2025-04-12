

--1. Obtener el promedio de precios por cada categoría de producto. La cláusula
--OVER(PARTITION BY CategoryID) específica que se debe calcular el promedio de
--precios por cada valor único de CategoryID en la tabla.

select 
c.category_name as name, 
p.product_name as product_name, 
p.unit_price as unite_price, 
AVG(p.unit_price) over(partition by c.category_id) as avg_unit_price
from categories c inner join products p 
on c.category_id =p.category_id

--2 Obtener el promedio de venta de cada cliente 
select
AVG(od.unit_price*od.quantity) OVER(partition by o.customer_id),
o.order_id  as Order_id, 
o.customer_id as customer_id, 
o.employee_id as employeed_id, 
o.order_date, 
o.required_date, 
o.shipped_date
from orders as o 
inner join order_details od  
on od.order_id =o.order_id

--3 Obtener el promedio de cantidad de productos vendidos por categoría (product_name,
--quantity_per_unit, unit_price, quantity, avgquantity) y ordenarlo por nombre de la
--categoría y nombre del producto

select 
p.product_name, 
c.category_name, 
p.quantity_per_unit, 
od.unit_price, 
AVG(od.quantity ) over (partition by p.product_name order by p.product_name asc, c.category_name asc ) as avgquantity
from products p inner join categories c 
on p.category_id =c.category_id
inner join order_details od
on od.product_id =p.product_id


--4. Selecciona el ID del cliente, la fecha de la orden y la fecha más antigua de la
--orden para cada cliente de la tabla 'Orders'.

select 
customer_id as customer_id, 
order_date, 
MIN(order_date ) over (partition by customer_id ) as MInOrderDate
from orders o 

--5.Seleccione el id de producto, el nombre de producto, el precio unitario, el id de
--categoría y el precio unitario máximo para cada categoría de la tabla Products.

select 
p.product_id, 
p.product_name, 
p.unit_price, 
c.category_id, 
MAX(p.unit_price) over (partition by c.category_id) as maxunitprice
from products as p 
inner join categories as c 
on p.category_id =c.category_id 


--6.Obtener el ranking de los productos más vendidos

select 

p.product_name, SUM(od.quantity), 
ROW_NUMBER() over (order by SUM(od.quantity) desc)

from products as p 
inner join order_details od 
on p.product_id =od.product_id
group by p.product_name 


--7.Asignar numeros de fila para cada cliente, ordenados por customer_id 

select 
ROW_NUMBER() over (order by c.customer_id), 
*
from customers c 


--8. Obtener el ranking de los empleados más jóvenes () ranking, nombre y apellido del
--empleado, fecha de nacimiento)

select concat(first_name, ' ', last_name ), birth_date,
row_number() over (order by age(date(now()), birth_date) ASC)
from employees e 


--9. Obtener la suma de venta de cada cliente

select 
o.order_id, 
o.customer_id, 
o.employee_id, 
SUM(od.unit_price*od.quantity) over (partition by o.customer_id) as sumorderamount,
order_date,  
required_date
from order_details as od  
inner join orders as o 
on od.order_id=o.order_id 


--10.  Obtener la suma total de ventas por categoría de producto

select 
c.category_name, 
p.product_name,
od.unit_price, 
od.quantity, 
SUM(od.unit_price*od.quantity) over (partition by c.category_name) as total_sales 
from categories c 
inner join products as p 
on p.category_id =c.category_id 
inner join order_details od  
on od.product_id =p.product_id


--11. Calcular la suma total de gastos de envío por país de destino, luego ordenarlo por país
--y por orden de manera ascendente


select 
ship_country as country, 
order_id as order_id, 
shipped_date , 
freight , 
SUM(freight ) over (partition by ship_country order by ship_country  asc)
from orders 
where shipped_date  is not null 


--12. Ranking de ventas por cliente

select 
c.customer_id, 
c.company_name,
SUM(od.unit_price*od.quantity),
RANK() over (order by SUM(od.unit_price*od.quantity) desc)
from customers c 
inner join orders as o 
on o.customer_id =c.customer_id 
inner join order_details od  
on od.order_id =o.order_id
group by c.customer_id, c.company_name


--13.  Ranking de empleados por fecha de contratacion

select 
employee_id, 
first_name, 
last_name, 
hire_date, 
RANK() OVER(order by hire_date asc )

from employees  

--14. Ranking de productos por precio unitario

select 
product_id, 
product_name, 
unit_price, 
rank() over(order by unit_price desc )
from products 


--15 . Mostrar por cada producto de una orden, la cantidad vendida y la cantidad
--vendida del producto previo.

select 
order_id, 
product_id, 
quantity, 
LAG(quantity) over (order by order_id, product_id )
from order_details 


--16. 6. Obtener un listado de ordenes mostrando el id de la orden, fecha de orden, id del cliente
--y última fecha de orden.

select 
order_id, 
order_date, 
customer_id, 
lag(order_date) over(partition by customer_id order by order_id)
 from orders 
 
 
 --17.  Obtener un listado de productos que contengan: id de producto, nombre del producto,
--precio unitario, precio del producto anterior, diferencia entre el precio del producto y
--precio del producto anterior.
 
 select 
 product_id, 
 product_name, 
 unit_price, 
 lag(unit_price) over (order by p.product_id),  
 unit_price-lag(unit_price) over(order by p.product_id)
  from products as p 
  
  
  --18. Obtener un listado que muestra el precio de un producto junto con el precio del producto
--siguiente
  select 
  p.product_name, 
  p.unit_price, 
  lead(p.unit_price) over(order by p.product_id) nextprice 
  from products p  
  
  
  --19. Obtener un listado que muestra el total de ventas por categoría de producto junto con el
--total de ventas de la categoría siguiente
  
  SELECT 
  c.category_name,
  SUM(od.unit_price * od.quantity) AS total_sales,
  LEAD(SUM(od.unit_price * od.quantity)) OVER (ORDER BY c.category_name) AS next_category_sales
FROM order_details od
INNER JOIN products p ON p.product_id = od.product_id
INNER JOIN categories c ON c.category_id = p.category_id
GROUP BY c.category_name
ORDER BY c.category_name;

  
 