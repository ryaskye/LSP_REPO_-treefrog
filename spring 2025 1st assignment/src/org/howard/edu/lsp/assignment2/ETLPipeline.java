package org.howard.edu.lsp.assignment2;

import java.io.*;
import java.util.*;
//got help for starter code from google and chat gpt for debugging https://www.google.com/search?q=example+code+of+how+to+read+a+csv+file+in+java&sca_esv=aed234037716436e&sxsrf=AHTn8zpC2OGr-N6lfZP2moo5sRvSB3Dyyg%3A1738975042636&ei=QqemZ-u_Jomf5NoP0vK--QE&ved=0ahUKEwjr6MHt6rKLAxWJD1kFHVK5Lx8Q4dUDCBA&uact=5&oq=example+code+of+how+to+read+a+csv+file+in+java&gs_lp=Egxnd3Mtd2l6LXNlcnAiLmV4YW1wbGUgY29kZSBvZiBob3cgdG8gcmVhZCBhIGNzdiBmaWxlIGluIGphdmEyCBAhGKABGMMESPEPUNcGWMMOcAJ4AZABAJgBiQGgAdMFqgEDMy40uAEDyAEA-AEBmAIIoAKMBcICChAAGLADGNYEGEfCAggQABiiBBiJBcICBRAAGO8FwgIIEAAYgAQYogTCAgoQIRigARjDBBgKmAMAiAYBkAYIkgcDNS4zoAfDHA&sclient=gws-wiz-serp
public class ETLPipeline {
    public static void main(String[] args) {
        System.out.println("Starting ETL pipeline..."); // Step 1: Debugging message

        // File paths
        String inputFilePath = "data/products.csv";
        String outputFilePath = "data/transformed_products.csv";

        // Store transformed data
        List<String[]> transformedData = new ArrayList<>();

        // Check if input file exists
        File file = new File(inputFilePath);
        if (!file.exists()) {
            System.err.println("Error: products.csv not found in data/ folder.");
            return; // Exit the program if file is missing
        } else {
            System.out.println("products.csv found! Proceeding with processing...");
        }

        try (BufferedReader br = new BufferedReader(new FileReader(inputFilePath))) {
            String line;
            boolean isHeader = true;

            System.out.println("Reading the file..."); // Debugging message

            while ((line = br.readLine()) != null) {
                System.out.println("Processing line: " + line); // Step 1: Print each line
                
                // Skip header but store it for output
                if (isHeader) {
                    transformedData.add(new String[]{"ProductID", "Name", "Price", "Category", "PriceRange"});
                    isHeader = false;
                    continue;
                }

                // Read CSV values
                String[] parts = line.split(",");
                if (parts.length != 4) {
                    System.out.println("Skipping invalid row: " + Arrays.toString(parts));
                    continue;
                }

                int productId = Integer.parseInt(parts[0]);
                String name = parts[1].toUpperCase();  // Convert name to uppercase
                double price = Double.parseDouble(parts[2]);
                String category = parts[3];

                // discount
                if (category.equals("Electronics")) {
                    price *= 0.90;
                    price = Math.round(price * 100.0) / 100.0; // Round to 2 decimal places
                }

                // Change category for electronics
                if (category.equals("Electronics") && price > 500) {
                    category = "Premium Electronics";
                }

                // For Price Range
                String priceRange;
                if (price <= 10) priceRange = "Low";
                else if (price <= 100) priceRange = "Medium";
                else if (price <= 500) priceRange = "High";
                else priceRange = "Premium";

                
                transformedData.add(new String[]{String.valueOf(productId), name, String.valueOf(price), category, priceRange});
            }

            //  Output CSV file
            try (BufferedWriter bw = new BufferedWriter(new FileWriter(outputFilePath))) {
                for (String[] row : transformedData) {
                    bw.write(String.join(",", row));
                    bw.newLine();
                }
            }

            System.out.println("Transformation complete! Check transformed_products.csv in the data folder.");
            
        } catch (FileNotFoundException e) {
            System.err.println("File not found: " + e.getMessage());
        } catch (IOException e) {
            System.err.println("I/O error: " + e.getMessage());
        }
    }
}
