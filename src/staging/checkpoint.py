import json
import logging
import os
import datetime
import shutil

class stateMachine:
    def __init__(self):
        # For batch
        self.current_batch=0 # Current batch being processed
        self.completed_batch = []
        self.current_status = "idle"
        self.failed_id = [] # Failed IDs
        
        # Metadata
        self.created_at = datetime.datetime.now().isoformat()
        self.last_updated = datetime.datetime.now().isoformat()
        self.total_products = 0

    def update_batch(self, just_complete_batch, current_batch, failed_id):
        self.completed_batch.append(just_complete_batch)
        self.current_batch = current_batch + 1
        self.failed_id = failed_id
        self.current_id = 0
        self.last_updated = datetime.datetime.now().isoformat()
    
    def add_failed(self, failed_ids):
        if isinstance(failed_ids, list):
            for failed_id in failed_ids:
                if failed_id not in self.failed_id:
                    logging.info(f"Adding failed ID: {failed_id}")
                    self.failed_id.append(failed_id)
                else:
                    logging.info(f"Failed ID {failed_id} is already in the list.")
        else:
            if failed_ids not in self.failed_id:
                logging.info(f"Adding failed ID: {failed_ids}")
                self.failed_id.append(failed_ids)
            else:
                logging.info(f"Failed ID {failed_ids} is already in the list.")

    def update_progress(self, current_id):
        self.current_id = current_id

    def update_status(self, current_status):
        self.current_status = current_status
        self.last_updated = datetime.datetime.now().isoformat()
        
    def get_state(self):
        return {
            "created_at": self.created_at,
            "last_updated": self.last_updated,
            "current_batch": self.current_batch,
            "completed_batch": self.completed_batch,
            "failed_id": self.failed_id,
            "current_status": self.current_status,
            "total_products": self.total_products
        }

def save_checkpoint(state, filename):
      filename=str(filename)
      temp_filename = filename + ".tmp"
      backup_filename = filename + ".bak"

      try:
          # Create backup of existing checkpoint
          if os.path.exists(filename):
              shutil.copy2(filename, backup_filename)

          # Write to temporary file
          with open(temp_filename, "w") as f:
              json.dump(state.get_state(), f, indent=2)

          # Verify the temp file was written correctly
          with open(temp_filename, "r") as f:
              json.load(f)  # Will raise exception if corrupted

          # Atomic replacement (on most filesystems)
          os.replace(temp_filename, filename)

          # Clean up backup after successful write
          if os.path.exists(backup_filename):
              os.remove(backup_filename)

      except Exception as e:
          # Restore from backup if something went wrong
          if os.path.exists(backup_filename):
              shutil.copy2(backup_filename, filename)
          logging.error(f"Failed to save checkpoint: {e}")
          raise
      finally:
          # Clean up temp file
          if os.path.exists(temp_filename):
              os.remove(temp_filename)

def load_checkpoint(filename):
    if os.path.exists(filename):
        with open(filename, "r") as f:
            return json.load(f)
    return None

def archive_checkpoint(filename):
    if os.path.exists(filename):
        archive_dir = os.path.join(os.path.dirname(filename), "archive")
        os.makedirs(archive_dir, exist_ok=True)
        
        # Create timestamp-based filename to avoid collisions
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S") # create_datetime
        base_name = os.path.splitext(os.path.basename(filename))[0] # File name without extension (without .json)
        archive_filename = f"{base_name}_{timestamp}.json"
        archive_path = os.path.join(archive_dir, archive_filename)
        
        shutil.copy2(filename, archive_path)
        os.remove(filename)  # Clean up original checkpoint
        logging.info(f"Checkpoint archived to {archive_path}")