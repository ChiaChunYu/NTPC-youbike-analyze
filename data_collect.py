import requests
import json
import time
import happybase

def main():
    connection = None
    try:
        connection = happybase.Connection('localhost')   
        table_name = 'Youbike_data'

        if table_name.encode() not in connection.tables():
            families = {
                'data': dict()  
            }
            connection.create_table(table_name, families)

        table = connection.table(table_name)
        connection.close()
        count = 0
        while True:
            count += 1
            print(count)
            try:
                response = requests.get('https://tcgbusfs.blob.core.windows.net/dotapp/youbike/v2/youbike_immediate.json', timeout=10)   
                response.raise_for_status() 
                data = json.loads(response.text)

                connection = happybase.Connection('localhost')
                table = connection.table(table_name)
                
                for record in data:
                    key = record['srcUpdateTime'] + record['sno']  
                    table.put(key.encode(), {b'data:info': json.dumps(record)}) 
                connection.close()
                if count == 97:
                    break
                time.sleep(900)  

            except requests.RequestException as e:
                print(f"network request error: {e}")
                time.sleep(900)  
            except json.JSONDecodeError as e:
                print(f"JSON parse error: {e}")
                time.sleep(900)  

    except Exception as e:
        print(f"error: {e}")
    finally:
        if connection is not None:
            connection.close()

if __name__ == "__main__":
    main()