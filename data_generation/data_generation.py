import psycopg2
from psycopg2.extras import execute_batch
import pandas as pd
import time
from datetime import datetime
import os
import random
import json
import logging


class InstacartDataLoader:
    """Class to handle loading Instacart data into PostgreSQL database"""
    
    def __init__(self, db_config, data_dir, log_file):
        """
        Initialize the data loader
        
        Args:
            db_config (dict): Database configuration parameters
            data_dir (str): Directory containing CSV data files
            log_file (str): Path to log file
        """
        self.db_config = db_config
        self.data_dir = data_dir
        self.logger = self._setup_logging(log_file)
    
    def _setup_logging(self, log_file):
        """Configure logging to file and console"""
        # Create logs directory if it doesn't exist
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        
        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, mode='a'),
                logging.StreamHandler()  # Also log to console for monitoring
            ]
        )
        return logging.getLogger(__name__)
    
    def get_db_connection(self):
        """Create and return a database connection"""
        try:
            conn = psycopg2.connect(**self.db_config)
            return conn
        except Exception as e:
            self.logger.error(f"Error connecting to database: {e}")
            raise
    
    def load_csv(self, filename):
        """Load CSV file into a pandas DataFrame"""
        filepath = os.path.join(self.data_dir, filename)
        self.logger.info(f"Loading {filename}...")
        df = pd.read_csv(filepath)
        self.logger.info(f"Loaded {len(df)} rows from {filename}")
        return df
    
    @staticmethod
    def convert_to_native_types(df):
        """Convert numpy dtypes to native Python types"""
        df = df.copy()
        for col in df.columns:
            if df[col].dtype.name.startswith('int'):
                df[col] = df[col].astype('Int64')  # Nullable integer type
            elif df[col].dtype.name.startswith('float'):
                df[col] = df[col].astype('float64')
        return df
    
    def insert_dataframe_batch(self, conn, df, table_name, columns, batch_size=1000):
        """Insert DataFrame into database table using batch insert"""
        cursor = conn.cursor()
        
        # Convert to native types
        df = self.convert_to_native_types(df)
        
        # Prepare the insert query
        placeholders = ','.join(['%s'] * len(columns))
        insert_query = f"INSERT INTO {table_name} ({','.join(columns)}) VALUES ({placeholders})"
        
        # Convert DataFrame to list of tuples with proper None handling
        data = []
        for _, row in df[columns].iterrows():
            tuple_data = tuple(None if pd.isna(val) else int(val) if isinstance(val, (pd.Int64Dtype, int)) and not isinstance(val, bool) 
                              else float(val) if isinstance(val, float) else val 
                              for val in row)
            data.append(tuple_data)
        
        # Execute batch insert
        execute_batch(cursor, insert_query, data, page_size=batch_size)
        conn.commit()
        cursor.close()
        self.logger.info(f"Inserted {len(data)} rows into {table_name}")
    
    @staticmethod
    def load_progress(tracking_file_path):
        """Load progress from log file"""
        if os.path.exists(tracking_file_path):
            with open(tracking_file_path, 'r') as f:
                return json.load(f)
        return {'last_order_index': 0, 'last_order_date': '2025-12-01'}
    
    @staticmethod
    def save_progress(progress, tracking_file_path):
        """Save progress to log file"""
        os.makedirs(os.path.dirname(tracking_file_path), exist_ok=True)
        with open(tracking_file_path, 'w') as f:
            json.dump(progress, f, indent=2)
    
    def insert_batch_with_orders(self, conn, orders_batch, batch_order_products):
        """Insert a batch of orders along with their order products"""
        cursor = conn.cursor()
        
        try:
            # Insert orders
            order_columns = [ 'order_id', 'user_id', 
                            'order_number', 'days_since_prior_order',
                            'order_date', 'order_hour_of_day'
                        ]
            
            # Convert orders batch to native types and prepare data
            orders_batch_native = self.convert_to_native_types(orders_batch)
            order_data = []
            for _, row in orders_batch_native[order_columns].iterrows():
                tuple_data = tuple(None if pd.isna(val) else int(val) if pd.notna(val) and isinstance(val, (int, float)) and not isinstance(val, bool)
                                  else val for val in row)
                order_data.append(tuple_data)
            
            placeholders = ','.join(['%s'] * len(order_columns))
            insert_query = f"INSERT INTO instacart.orders ({','.join(order_columns)}) VALUES ({placeholders})"
            execute_batch(cursor, insert_query, order_data, page_size=len(order_data))
            
            if len(batch_order_products) > 0:
                # Insert order products
                order_product_columns = ['order_id', 'product_id', 'add_to_cart_order', 'reordered']
                
                # Convert to native types and prepare data
                batch_order_products_native = self.convert_to_native_types(batch_order_products)
                product_data = []
                for _, row in batch_order_products_native[order_product_columns].iterrows():
                    tuple_data = tuple(int(val) if pd.notna(val) else None for val in row)
                    product_data.append(tuple_data)
                
                placeholders = ','.join(['%s'] * len(order_product_columns))
                insert_query = f"INSERT INTO instacart.order_products ({','.join(order_product_columns)}) VALUES ({placeholders})"
                execute_batch(cursor, insert_query, product_data, page_size=len(product_data))
            
            conn.commit()
            cursor.close()
            
            return len(order_data), len(batch_order_products)
            
        except Exception as e:
            conn.rollback()
            cursor.close()
            raise e
    
    def reset_progress(self, tracking_file_path):
        """Reset the progress log to start from the beginning"""
        if os.path.exists(tracking_file_path):
            os.remove(tracking_file_path)
            self.logger.info("Progress log reset. Next run will start from the beginning.")
        else:
            self.logger.info("No progress log found.")
    
    def load_initial(self):
        """Load all dimension tables (aisles, departments, products)"""
        conn = self.get_db_connection()
        
        try:
            self.logger.info("="*60)
            self.logger.info("LOADING DIMENSION TABLES")
            self.logger.info("="*60)
            
            # Load aisles
            self.logger.info("Loading aisles...")
            aisles_df = self.load_csv('aisles.csv')
            self.insert_dataframe_batch(conn, aisles_df, 'instacart.aisles', ['aisle_id', 'aisle'])
            
            # Load departments
            self.logger.info("Loading departments...")
            departments_df = self.load_csv('departments.csv')
            self.insert_dataframe_batch(conn, departments_df, 'instacart.departments', ['department_id', 'department'])
            
            # Load products
            self.logger.info("Loading products...")
            products_df = self.load_csv('products.csv')
            self.insert_dataframe_batch(conn, products_df, 'instacart.products', 
                                  ['product_id', 'product_name', 'aisle_id', 'department_id'],
                                  batch_size=100000)
            
            self.logger.info("="*60)
            self.logger.info("DIMENSION TABLES LOADED SUCCESSFULLY")
            self.logger.info("="*60)

            self.logger.info("="*60)
            self.logger.info("LOADING FACT TABLES")
            self.logger.info("="*60)            

            # load orders with dates
            self.logger.info("Loading orders with dates...")
            orders_with_dates_df = self.load_csv('orders_with_dates.csv')
            orders_with_dates_df = orders_with_dates_df[
                (pd.to_datetime(orders_with_dates_df['order_date']) >= pd.to_datetime('2025-12-20')) &
                (pd.to_datetime(orders_with_dates_df['order_date']) < pd.to_datetime('today').normalize())]
            self.logger.info(f"Filtered orders to {len(orders_with_dates_df)} rows with order_date between 2025-12-01 and {pd.Timestamp('today').normalize()}")
            self.insert_dataframe_batch(conn, orders_with_dates_df, 'instacart.orders', 
                                [ 'order_id', 'user_id', 
                                'order_number', 'days_since_prior_order',
                                'order_date', 'order_hour_of_day'
                                ], batch_size=100000)
            
            train_products_df = pd.read_csv(os.path.join(self.data_dir, 'order_products__train.csv'))
            prior_products_df = pd.read_csv(os.path.join(self.data_dir, 'order_products__prior.csv'))
            full_order_products_df = pd.concat([train_products_df, prior_products_df], ignore_index=True)
            
            order_ids = [int(x) for x in orders_with_dates_df['order_id'].tolist()]
            batch_order_products = full_order_products_df[full_order_products_df['order_id'].isin(order_ids)]
            self.logger.info(f"Filtered orders to {len(batch_order_products)} order products matching loaded orders")
            
            self.insert_dataframe_batch(conn, batch_order_products, 'instacart.order_products', 
                                ['order_id', 'product_id', 'add_to_cart_order', 'reordered']
                                , batch_size=200000)

            self.logger.info("="*60)
            self.logger.info("FACT TABLES LOADED SUCCESSFULLY")
            self.logger.info("="*60)

        except Exception as e:
            self.logger.error(f"Error loading dimension tables: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()
    
    def load_orders_incrementally(
            self,
            tracking_file_path,
            min_orders_per_batch=10,
            max_orders_per_batch=50,
            min_sleep_seconds=5,
            max_sleep_seconds=15):
        """Load train orders and order products incrementally to simulate OLTP behavior"""
        conn = self.get_db_connection()
        
        try:
            self.logger.info("="*60)
            self.logger.info("LOADING ORDERS DATA (INCREMENTALLY)")
            self.logger.info("="*60)
            
            # Load progress
            progress = self.load_progress(tracking_file_path)
            latest_order_date = progress.get('last_order_date', '2025-12-01')
            start_index = progress.get('last_order_index', 0)
            
            if start_index > 0:
                self.logger.info(f"Resuming from order_date: {latest_order_date}, incremental_index: {start_index}")
            
            self.logger.info(f"Loading orders from {latest_order_date} onwards...")
            
            # Load orders
            self.logger.info("Loading orders_with_dates.csv...")
            orders_df = pd.read_csv(os.path.join(self.data_dir, 'orders_with_dates.csv'))
            
            # Filter orders based on order_date and incremental_index
            orders_df = orders_df[
                (pd.to_datetime(orders_df['order_date']) > pd.to_datetime(latest_order_date).normalize()) |
                ((pd.to_datetime(orders_df['order_date']) == pd.to_datetime(latest_order_date).normalize()) & 
                 (orders_df['incremental_index'] > start_index))
            ].sort_values(['order_date', 'incremental_index']).reset_index(drop=True)
            
            self.logger.info(f"Found {len(orders_df)} orders to process")
            
            # If no orders found, move checkpoint to next day
            if len(orders_df) == 0:
                next_date = (pd.to_datetime(latest_order_date) + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
                progress['last_order_date'] = next_date
                progress['last_order_index'] = 0
                self.save_progress(progress, tracking_file_path)
                self.logger.info(f"No orders found for {latest_order_date}. Moving checkpoint to {next_date}")
                conn.close()
                return
            
            # Load order products
            self.logger.info("Loading test_batch_order_products.csv...")
            full_order_products_df = pd.read_csv(os.path.join(self.data_dir, 'test_batch_order_products.csv'))
            self.logger.info(f"Found {len(full_order_products_df)} order products")
            
            # Process in batches with random sizes
            total_orders = len(orders_df)
            max_incremental_index = int(orders_df['incremental_index'].max()) if len(orders_df) > 0 else start_index
            current_incremental_index = start_index
            processed_count = 0
            
            self.logger.info(f"Incremental index range: {start_index} -> {max_incremental_index}")
            
            try:
                while processed_count < total_orders:
                    # Random batch size
                    batch_size = random.randint(min_orders_per_batch, max_orders_per_batch)
                    end_count = min(processed_count + batch_size, total_orders)
                    
                    # Get batch of orders
                    orders_batch = orders_df.iloc[processed_count:end_count]
                    order_ids = [int(x) for x in orders_batch['order_id'].tolist()]
                    batch_order_products = full_order_products_df[full_order_products_df['order_id'].isin(order_ids)]
                    
                    # Insert orders and their products
                    orders_inserted, products_inserted = self.insert_batch_with_orders(
                        conn, orders_batch, batch_order_products
                    )
                    
                    # Update progress with the latest order_date and incremental_index from this batch
                    last_order_in_batch = orders_batch.iloc[-1]
                    progress['last_order_date'] = str(last_order_in_batch['order_date'])
                    progress['last_order_index'] = int(last_order_in_batch['incremental_index'])
                    current_incremental_index = progress['last_order_index']
                    self.save_progress(progress, tracking_file_path)
                    
                    # Update processed counter
                    processed_count = end_count
                    
                    # Calculate progress percentage based on incremental_index
                    if max_incremental_index > start_index:
                        index_progress = ((current_incremental_index - start_index) / (max_incremental_index - start_index)) * 100
                    else:
                        index_progress = 100.0
                    
                    # Log progress
                    self.logger.info(f"Batch completed: {orders_inserted} orders, {products_inserted} order products | "
                               f"Latest: order_date={progress['last_order_date']}, index={current_incremental_index}/{max_incremental_index} ({index_progress:.1f}%) | ")
                    
                    # Sleep with random duration to simulate real-time data generation
                    if processed_count < total_orders:
                        sleep_time = random.randint(min_sleep_seconds, max_sleep_seconds)
                        self.logger.info(f"Sleeping for {sleep_time} seconds...")
                        time.sleep(sleep_time)
                
                self.logger.info("="*60)
                self.logger.info("TRAIN ORDERS DATA LOADED SUCCESSFULLY")
                self.logger.info(f"Final checkpoint: order_date={progress['last_order_date']}, index={progress['last_order_index']}")
                self.logger.info("="*60)
                
            except KeyboardInterrupt:
                self.logger.warning("="*60)
                self.logger.warning("⚠️  INTERRUPTED BY USER")
                self.logger.warning("="*60)
                self.logger.info(f"Progress saved at: order_date={progress['last_order_date']}, index={current_incremental_index}/{max_incremental_index}")
                self.logger.info(f"Remaining orders: {total_orders - processed_count}")
                self.logger.info("You can safely re-run this cell to continue from where you left off.")
                self.logger.warning("="*60)
            
        except Exception as e:
            self.logger.error(f"Error loading train orders: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()
