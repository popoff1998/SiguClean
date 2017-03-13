TODO
====

*(última edición 13/3/2017)*

1. ¿Como distinguimos un usuario que falló al archivarse de uno que se archivo pero no tenía storages por lo que no ha creado entrada en **UT_ST_STORAGES**?

2. ¿Porqué **UT_SF_ULTIMA** no funciona cuando el usuario no está caducado o cancelado?

3. ¿Cuando se cambia el login de una cuenta, debemos marcar la antigua con un estado diferente de cancelado?
* Los cambios de login mantienen en el storage el nombre del antiguo. Como la cuenta antigua pasa a estar cancelada es archivable por lo que se borrarán los storages. Esto causa que el nuevo login pierda el acceso a cosas como el correo por ejemplo.

4. ¿Es interesante crear una nueva tabla (**UT_ST_CUENTAS**) donde haya una entrada por cada usuario y se mantenga el estado del mismo?
* Cuando ejecutamos siguclean generamos información en las cuentas que fallan que solo queda reflejada en los logs pero no en sigu. Una tabla como esta permitiría por ejemplo solucionar el punto 1) pues podríamos almacenar información de que el usuario se procesó para archivar pero como no tenía storages no generó información en **UT_ST_STORAGES**

5. Implementar la opción de que en interactivo los comandos que generan salida de usuario la manden a un fichero en el tmp.
