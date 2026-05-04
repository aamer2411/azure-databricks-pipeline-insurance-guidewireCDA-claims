# Pipeline Screenshots

Visual reference for the Azure Data Factory pipelines, Databricks workspace, and Azure Data Lake Storage layout.

To add screenshots: save your `.png` files into this `docs/` folder and they will render automatically below. GitHub renders images embedded in Markdown files directly in the browser.

---

## ADF - Master Pipeline

The full end-to-end orchestration view showing the `pl_m_source_to_refined` pipeline canvas.

![Master Pipeline - pl_m_source_to_refined](adf_master_pipeline.png)

> How to take: Open Azure Data Factory Studio, navigate to Author > Pipelines > `pl_m_source_to_refined`, and screenshot the canvas.

---

## ADF - Source to Raw Child Pipeline

`pl_c_source_to_raw` showing the manifest notebooks and ForEach Source-to-Raw activity chain.

![Source to Raw Pipeline](adf_source_to_raw.png)

> How to take: Open `pl_c_source_to_raw` canvas in ADF Studio.

---

## ADF - Raw to Replica Child Pipeline

`pl_c_raw_to_replica` showing the ForEach parallelism and balancing notebook chain.

![Raw to Replica Pipeline](adf_raw_to_replica.png)

> How to take: Open `pl_c_raw_to_replica` canvas in ADF Studio.

---

## ADF - Replica to Refined Child Pipeline

`pl_c_replica_to_refined` showing the SCD Type 2 ForEach and the prev-cycle lookup.

![Replica to Refined Pipeline](adf_replica_to_refined.png)

> How to take: Open `pl_c_replica_to_refined` canvas in ADF Studio.

---

## ADF - Pipeline Run History

Monitoring view showing successful end-to-end pipeline runs.

![ADF Pipeline Run History](adf_run_history.png)

> How to take: In ADF Studio, go to Monitor > Pipeline runs. Filter by `pl_m_source_to_refined` and screenshot a successful run.

---

## Unity Catalog - Schema Browser

Databricks Unity Catalog showing the `insurance_claims_domain` catalog with `replica`, `refined`, and `analytics` schemas and their tables.

![Unity Catalog Schema Browser](unity_catalog_schemas.png)

> How to take: In Databricks, click Catalog in the left sidebar, expand `insurance_claims_domain`.

---

## ADLS - Folder Structure

Azure Storage browser showing the `raw/`, `replica/`, and `refined/` folder hierarchy inside the `insurance-claims-domain` container.

![ADLS Folder Structure](adls_folder_structure.png)

> How to take: In Azure Portal, open your storage account, go to Storage browser > Blob containers > `insurance-claims-domain`.

---

## Notebook 07 - CDC MERGE Output

Replica load notebook showing the MERGE result with INSERT/UPDATE/soft-delete counts per table.

![Notebook 07 - Raw to Replica MERGE Output](notebook_07_merge_output.png)

> How to take: Run notebook 07 against one of the 5 tables and screenshot the final metrics cell output.

---

## Notebook 12 - SCD Type 2 MERGE Output

Refined load notebook showing the generated SCD2 MERGE SQL and the ROWS_READ / ROWS_INSERTED / ROWS_UPDATED reconciliation.

![Notebook 12 - SCD Type 2 MERGE Output](notebook_12_scd2_output.png)

> How to take: Run notebook 12 for one of the two refined tables and screenshot the MERGE query print and the metrics cells.
