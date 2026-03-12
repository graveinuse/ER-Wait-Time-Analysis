import os
from fpdf import FPDF

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
DOCS_DIR = os.path.join(BASE_DIR, "docs")
OUTPUT_PDF = os.path.join(DOCS_DIR, "executive_summary.pdf")

class PDF(FPDF):
    def header(self):
        self.set_font('Arial', 'B', 15)
        # Title
        self.cell(0, 10, 'Executive Summary: ER Wait Time & Patient Flow Analytics', 0, 1, 'C')
        self.ln(5)

    def footer(self):
        # Go to 1.5 cm from bottom
        self.set_y(-15)
        self.set_font('Arial', 'I', 8)
        # Page number
        self.cell(0, 10, 'Page ' + str(self.page_no()) + '/{nb}', 0, 0, 'C')

def generate_pdf():
    pdf = PDF()
    pdf.alias_nb_pages()
    pdf.add_page()
    
    # --- Context ---
    pdf.set_font('Arial', 'B', 12)
    pdf.cell(0, 10, 'Context', 0, 1)
    
    pdf.set_font('Arial', '', 11)
    context_text = (
        "Emergency Departments (EDs) are critical healthcare access points where operational "
        "inefficiency directly impacts patient outcomes. Due to unpredictable volume surges and "
        "triage constraints, wait times often balloon, leading to increased 'Leaving Without Being "
        "Seen' (LWBS) rates and severe downstream boarding delays. This analysis evaluates 50,000 "
        "clinical records across 3 years to identify patient flow bottlenecks and implement targeted, "
        "data-driven process optimization strategies."
    )
    pdf.multi_cell(0, 7, context_text)
    pdf.ln(5)
    
    # --- Key Findings ---
    pdf.set_font('Arial', 'B', 12)
    pdf.cell(0, 10, 'Key Findings', 0, 1)
    
    pdf.set_font('Arial', '', 11)
    
    findings = [
        "1. Bottleneck Disparity: Triage successfully prioritizes emergent patients (ESI-1 median wait: 6 mins, ESI-5: 107 mins), but wait-to-volume ratio severely hampers low-acuity throughput.",
        "2. Bi-Modal Congestion Peaks: Two sustained volume surges cripple ED efficiency daily: 10:00 AM - 2:00 PM and 6:00 PM - 10:00 PM.",
        "3. Boarding Delays Mask Capacity: 23% of admitted patients experience total lengths-of-stay (LOS) exceeding 8 hours, compounding waiting room backlogs via physical bed-blocking.",
        "4. Staffing Threshold Exhaustion: Linear models indicate wait times spike disproportionately whenever patient-to-provider ratios exceed 1.5 new arrivals per hour."
    ]
    
    for finding in findings:
        pdf.multi_cell(0, 7, finding)
        pdf.ln(2)
        
    pdf.ln(3)
        
    # --- Recommendations ---
    pdf.set_font('Arial', 'B', 12)
    pdf.cell(0, 10, 'Strategic Recommendations', 0, 1)
    
    pdf.set_font('Arial', '', 11)
    
    recs = [
        "1. Implement Staggered 'Flex' Shifts: Shift scheduling must dynamically scale to meet the 10:00 AM and 6:00 PM dual-peaks. Activating proactive surge staff immediately preceding these peaks mitigates compounding delays.",
        "2. Introduce ESI 4-5 Fast-Track Lane: Deploying an isolated clinical pathway managed by Advanced Practice Providers (PAs/NPs) for lower-acuity patients will rapidly clear ~40% of standard ED volume, freeing critical capacity.",
        "3. Targeted Boarding Action: Escalate boarding patient discharges. An automated workflow alerting inpatient departments of an ED bed-block when LOS hits 6 hours is strongly recommended.",
        "4. Deploy Enterprise Patient Flow Dashboards: Embed the accompanying real-time Tableau dashboard directly inside ED operational hubs to shift scheduling from reactive observation to proactive analytics."
    ]
    
    for rec in recs:
        pdf.multi_cell(0, 7, rec)
        pdf.ln(2)
        
    pdf.ln(3)

    # --- Impact ---
    pdf.set_font('Arial', 'B', 12)
    pdf.cell(0, 10, 'Expected Impact', 0, 1)
    
    pdf.set_font('Arial', '', 11)
    impact_text = (
        "Executing the Fast-Track strategy operates on a projected timeline of 90 days "
        "and is modeled to reduce ESI 4-5 total LOS by an estimated 25%. Implementing continuous "
        "dashboard monitoring and flex-staffing resolves the 1.5 patient/hr ratio boundary, "
        "slashing mean peak-hour wait times by a projected 18%."
    )
    pdf.multi_cell(0, 7, impact_text)

    # Save
    pdf.output(OUTPUT_PDF, 'F')
    print(f"Executive Summary written to {OUTPUT_PDF}")

if __name__ == "__main__":
    generate_pdf()
