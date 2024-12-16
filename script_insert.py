import csv
import time
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, SimpleStatement, dict_factory


cluster = Cluster(['localhost'], port=9050)
session = cluster.connect('atelier')  


table_name = 'table_fait'

def insert_data(csv_file, batch_size=60):
   
    insert_query = f"""
    INSERT INTO {table_name} (
        fact_key, order_key, product_key, customer, product, supplier, dim_time, orderdate, priority, 
        ship_priority, quantity_ordered, extended_price, order_totalprice, 
        discount, revenue, supp_cost, tax, shipmode
    ) VALUES (?, ?, ?, 
        {{
            name: ?, phone: ?, city: ?, nation: ?, region: ?, address: ?, mktsegment: ?}}, 
        {{
            partkey: ?, name: ?, mfgr: ?, category: ?,  brand: ?, color: ?, type: ?, 
            size: ?, container: ?}}, 
        {{
            supplier_key: ?, name: ?, phone: ?, city: ?, nation: ?, region: ?, address: ?}}, 
        {{
            full_date: ?, day_of_week: ?, month: ?, year: ?, year_month: ?, month_year: ?, day_of_month: ?, 
            day_of_week_number: ?, week_of_year: ?, quarter: ?, holiday_flag: ?, 
            season: ?, is_weekend: ?, is_workday: ?, is_holiday: ?, is_peak_season: ?
        }}, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    prepared = session.prepare(insert_query)

    start_time = time.time()
    total_rows = 0
    successful_rows = 0
    failed_rows = 0
    batch_count = 0
    
    with open(csv_file, 'r', encoding='utf-8') as file:
        csv_reader = csv.reader(file, delimiter=';')
        next(csv_reader)  
        
        # Initialiser le batch
        batch = BatchStatement()
        
        for row in csv_reader:
            total_rows += 1
            try:
                params = (
                    int(row[0]),int(row[1]),int(row[2]),
                    # Customer
                    row[3],row[4],row[5],row[7], row[6],row[8], row[9], 
                    # Product details 
                    int(row[10]), row[11],row[12],row[13], row[14], row[15],row[16], int(row[17]),row[18],       
                    # Supplier details 
                    int(row[19]),row[20],row[21],row[22], row[24],row[23],row[25],       
                    # Time dimension
                    row[26],row[27],row[28],int(row[29]),row[30],row[31], int(row[32]),
                    int(row[33]),int(row[34]),int(row[35]),int(row[36]),row[37], int(row[38]),
                    int(row[39]), int(row[40]), int(row[41]), 
                    # fields
                    row[42].strip(),row[43],int(row[44]),int(row[45]), int(row[46]), int(row[47]),          
                    int(row[48]),int(row[49]),int(row[50]),int(row[51]),row[52]           
                )
                batch.add(prepared, params)
                successful_rows += 1
                
            
                if len(batch) >= batch_size:
                    session.execute(batch)
                    batch_count += 1
                    batch = BatchStatement()
                
            except (ValueError, IndexError) as e:
                print(f"Error processing row: {e}")
                failed_rows += 1
                continue
        
        
        if len(batch) > 0:
            session.execute(batch)
            batch_count += 1
    
    
    end_time = time.time()
    total_time = end_time - start_time
    

    print("\n--- Résumé de l'insertion ---")
    print(f"Temps total de chargement: {total_time:.2f} secondes")
    print(f"Nombre total de lignes: {total_rows}")
    print(f"Lignes insérées avec succès: {successful_rows}")
    print(f"Lignes en échec: {failed_rows}")
    print(f"Nombre de batches: {batch_count}")
    print(f"Taille moyenne du batch: {successful_rows/batch_count:.2f} lignes")
    print(f"Taux de succès: {(successful_rows/total_rows)*100:.2f}%")
    print(f"Débit: {successful_rows/total_time:.2f} lignes/seconde")

# Exécution du script
if __name__ == "__main__":
    
    CSV_FILE = 'nlineorder.csv'
    BATCH_SIZE = 60  # Taille du batch configurable
    
    try:
        insert_data(CSV_FILE, BATCH_SIZE)
        print("Terminé avec succès")
    except Exception as e:
        print(f"Une erreur s'est produite : {e}")
    finally:
        cluster.shutdown()