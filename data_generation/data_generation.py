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
    
    def insert_dataframe_batch(self, conn, df, table_name, columns):
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
        execute_batch(cursor, insert_query, data, page_size=1000)
        conn.commit()
        cursor.close()
        self.logger.info(f"Inserted {len(data)} rows into {table_name}")
    
    @staticmethod
    def load_progress(tracking_file_path):
        """Load progress from log file"""
        if os.path.exists(tracking_file_path):
            with open(tracking_file_path, 'r') as f:
                return json.load(f)
        return {'last_order_index': 0, 'total_orders_loaded': 0, 'total_order_products_loaded': 0}
    
    @staticmethod
    def save_progress(progress, tracking_file_path):
        """Save progress to log file"""
        os.makedirs(os.path.dirname(tracking_file_path), exist_ok=True)
        with open(tracking_file_path, 'w') as f:
            json.dump(progress, f, indent=2)
    
    def insert_batch_with_orders(self, conn, orders_batch, order_products_df):
        """Insert a batch of orders along with their order products"""
        cursor = conn.cursor()
        
        try:
            # Insert orders
            order_columns = ['order_id', 'user_id', 'order_number', 
                            'order_dow', 'order_hour_of_day', 'days_since_prior_order']
            
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
            
            # Get order products for this batch - convert order_ids to Python int
            order_ids = [int(x) for x in orders_batch['order_id'].tolist()]
            batch_order_products = order_products_df[order_products_df['order_id'].isin(order_ids)]
            
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
    
    def load_dimension_tables(self):
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
                                  ['product_id', 'product_name', 'aisle_id', 'department_id'])
            
            self.logger.info("="*60)
            self.logger.info("DIMENSION TABLES LOADED SUCCESSFULLY")
            self.logger.info("="*60)
            
        except Exception as e:
            self.logger.error(f"Error loading dimension tables: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()
    
    def load_train_orders_incrementally(
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
            self.logger.info("LOADING TRAIN ORDERS DATA (INCREMENTALLY)")
            self.logger.info("="*60)
            
            # Load progress
            progress = self.load_progress(tracking_file_path)
            start_index = progress['last_order_index']
            
            if start_index > 0:
                self.logger.info(f"Resuming from order index {start_index}")
                self.logger.info(f"Previously loaded: {progress['total_orders_loaded']} orders, "
                           f"{progress['total_order_products_loaded']} order products")
            
            # Load orders
            self.logger.info("Loading orders.csv...")
            orders_df = pd.read_csv(os.path.join(self.data_dir, 'orders.csv'))
            
            # Filter only train orders
            train_orders = orders_df[orders_df['eval_set'] == 'train'].sort_values('order_id').reset_index(drop=True)
            self.logger.info(f"Found {len(train_orders)} train orders")
            
            # Load order products
            self.logger.info("Loading order_products__train.csv...")
            train_products_df = pd.read_csv(os.path.join(self.data_dir, 'order_products__train.csv'))
            self.logger.info(f"Found {len(train_products_df)} train order products")
            
            # Process in batches with random sizes
            current_index = start_index
            total_orders = len(train_orders)
            
            try:
                while current_index < total_orders:
                    # Random batch size
                    batch_size = random.randint(min_orders_per_batch, max_orders_per_batch)
                    end_index = min(current_index + batch_size, total_orders)
                    
                    # Get batch of orders
                    orders_batch = train_orders.iloc[current_index:end_index]
                    
                    # Insert orders and their products
                    orders_inserted, products_inserted = self.insert_batch_with_orders(
                        conn, orders_batch, train_products_df
                    )
                    
                    # Update progress
                    current_index = end_index
                    progress['last_order_index'] = current_index
                    progress['total_orders_loaded'] += orders_inserted
                    progress['total_order_products_loaded'] += products_inserted
                    self.save_progress(progress, tracking_file_path)
                    
                    # Log progress
                    self.logger.info(f"Train batch completed: {orders_inserted} orders, {products_inserted} order products | "
                               f"Progress: {current_index}/{total_orders} orders ({current_index/total_orders*100:.1f}%)")
                    
                    # Sleep with random duration to simulate real-time data generation
                    if current_index < total_orders:
                        sleep_time = random.randint(min_sleep_seconds, max_sleep_seconds)
                        self.logger.info(f"Sleeping for {sleep_time} seconds...")
                        time.sleep(sleep_time)
                
                self.logger.info("="*60)
                self.logger.info("TRAIN ORDERS DATA LOADED SUCCESSFULLY")
                self.logger.info(f"Total orders loaded: {progress['total_orders_loaded']}")
                self.logger.info(f"Total order products loaded: {progress['total_order_products_loaded']}")
                self.logger.info("="*60)
                
            except KeyboardInterrupt:
                self.logger.warning("="*60)
                self.logger.warning("⚠️  INTERRUPTED BY USER")
                self.logger.warning("="*60)
                self.logger.info(f"Progress saved at order index: {progress['last_order_index']}")
                self.logger.info(f"Total orders loaded so far: {progress['total_orders_loaded']}")
                self.logger.info(f"Total order products loaded so far: {progress['total_order_products_loaded']}")
                self.logger.info(f"Remaining orders: {total_orders - current_index}")
                self.logger.info("You can safely re-run this cell to continue from where you left off.")
                self.logger.warning("="*60)
            
        except Exception as e:
            self.logger.error(f"Error loading train orders: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()
