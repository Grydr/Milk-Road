import pandas as pd

def correct_categories():
    # Read the original categories mapping file
    categories_real = pd.read_csv('categories_real.csv')
    
    # Create a dictionary for quick lookup of correct category names
    category_map = dict(zip(categories_real['categories_id'], categories_real['categories_name']))
    
    # Read the file that needs correction
    categories_revised = pd.read_csv('categories_revised.csv')
    
    # Create a new column with corrected category names using the mapping
    categories_revised['categories_name'] = categories_revised['categories_id'].map(category_map)
    
    # Save the corrected data to a new CSV file
    categories_revised.to_csv('categories_revised_corrected.csv', index=False)

# Run the correction function
if __name__ == "__main__":
    try:
        correct_categories()
        print("Categories have been successfully corrected and saved to 'categories_revised_corrected.csv'")
    except Exception as e:
        print(f"An error occurred: {str(e)}")