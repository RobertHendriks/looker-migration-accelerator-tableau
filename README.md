# **Looker Enterprise Migration Accelerator (LEMA)**

**Developed for Looker Partners to accelerate the migration of complex Tableau Workbooks into modern LookML projects.**

LEMA is a proprietary tool designed to automate the painful process of converting Tableau data source metadata (especially Custom SQL and complex aggregations) into high-quality, consolidated LookML View, Model, and Dashboard files.

## **✨ Gold Standard Features for Partners**

This utility meets the Gold Standard requirements for enterprise migration tools:

1. **Consolidated Architecture:** Avoids 1:1 file translation by consolidating related data sources into fewer, reusable LookML Views.  
2. **Adaptive Visualization:** Dynamically maps TWB visualization types (e.g., Line, Map) to the corresponding native Looker dashboard element types (looker\_line, looker\_geo\_coordinates).  
3. **Correct SQL Generation:** Automatically converts Tableau Custom SQL to LookML derived\_table blocks, maintaining data logic.

## **📊 Complexity Scoring (C-Score)**

A partner implementation must include a transparent and governable Complexity Scoring (C-Score) mechanism. While the current tool uses a simplified model, partners should customize this for their own scoping and risk assessment.

| Component | Purpose in Migration | Current Status |
| :---- | :---- | :---- |
| **View Consolidation** | Measures potential LookML reuse and simplification. | **Automated** |
| **LOD/Calc Analysis** | Pinpoints required engineering effort for advanced measure logic. | **Flagged for Manual Review** |
| **SQL/Data Blending** | Assesses risk associated with migrating complex data logic (Custom SQL). | **Automated (Generated as PDT/Derived Table)** |

## **🚀 Setup and Installation**

### **Prerequisites**

* **Python 3.8+**  
* **Git** (for cloning this repository)

### **Steps**

1. **Clone the Repository:**  
   git clone \[YOUR\_REPOSITORY\_URL\]  
   cd looker-migration-web

2. Create and Activate Virtual Environment:  
   This isolates your project dependencies.  
   python3 \-m venv venv  
   source venv/bin/activate

3. **Install Dependencies:**  
   pip install \-r requirements.txt

   *(Note: The actual TWB parsing often relies on the vendor-specific Tableau Migration SDK or external libraries. For this version, we use an XML adapter integrated into utility.py.)*  
4. **Start the Flask Web Server:**  
   python3 app.py

   The server will start on http://127.0.0.1:5000.

## **💻 Usage**

1. Open your browser to http://127.0.0.1:5000.  
2. Drag and drop your Tableau Workbook (.twb) files into the designated area.  
3. Click **Run Migration**.  
4. The output files (.model.lkml, .view.lkml, .dashboard.lookml, GOVERNANCE\_REVIEW.md) will be generated inside the local **./output/migration\_result/lookml/** directory.

## **🗃️ Key Files**

| Filename | Description |
| :---- | :---- |
| app.py | **Flask Server:** Handles file uploads and orchestrates Python execution. |
| utility.py | **Core Logic:** Contains all consolidation, hashing, naming, and LookML generation methods. |
| index.html | The web interface frontend. |
| requirements.txt | Python dependency manifest. |

