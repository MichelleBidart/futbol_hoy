def delete_table_from_redshift(conn, table_name: str, schema: str) -> None:
    """
    Elimina todos los registros de una tabla en Redshift.

    Args:
        conn: Conexión activa a la base de datos Redshift.
        table_name (str): Nombre de la tabla de la cual se eliminarán los registros.
        schema (str): Esquema donde se encuentra la tabla.
    """
    with conn.cursor() as cursor:
        cursor.execute(f'DELETE FROM "{schema}"."{table_name}"')  # Ejecuta el DELETE en la tabla
        conn.commit()  # Confirma la transacción
        print(f"Datos eliminados de la tabla {schema}.{table_name}.")
