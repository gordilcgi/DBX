-- Databricks notebook source
-- DBTITLE 1,Company Address Variations Data Initialization
CREATE OR REPLACE TEMP VIEW raw_data AS
SELECT * FROM VALUES
  ('CGI Alabama Birmingham','1900 International Park Drive, Suite 400, Birmingham, AL 35243', '+1 205-259-2300', NULL),
  ('CGI Alabama Birmingham','1900 International Park Dr., STE 400, Birmingham, AL 35243', '+1 205-259-2300', NULL),
  ('CGI Alabama Birmingham','1900 International Park DR, Unit 400, Birmingham, AL 35243', '+1 205-259-2300', NULL),
  ('CGI Alabama Huntsville','4000 Market Street, Huntsville, AL 35808', '+1 256-922-2400', '+1 256-534-6473'),
  ('CGI Alabama Huntsville','4000 Market St, Huntsville, AL 35808', '+1 256-922-2400', '+1 256-534-6473'),
  ('CGI Alabama Huntsville','4000 Market ST., Huntsville, AL 35808', '+1 256-922-2400', '+1 256-534-6473'),
  ('CGI Alabama Mobile','775 N. University Boulevard, Suite 250, Mobile, AL 36608', '1-251-706-4460', NULL),
  ('CGI Alabama Mobile','775 N. University Blvd, STE 250, Mobile, AL 36608', '1-251-706-4460', NULL),
  ('CGI Alabama Mobile','775 N. University Blvd., Unit 250, Mobile, AL 36608', '1-251-706-4460', NULL),
  ('CGI Alabama Troy','140 University Park, Troy, AL 36081', '+1 334-807-5540', '+1 334-566-1205'),
  ('CGI Alabama Troy','140 University Park Rd, Troy, AL 36081', '+1 334-807-5540', '+1 334-566-1205'),
  ('CGI Alabama Troy','140 University Park ROAD, Troy, AL 36081', '+1 334-807-5540', '+1 334-566-1205'),
  ('CGI Alaska Juneau','175 South Franklin Street, Suite 305, Juneau, AK 99801', '+1 907-463-4112 x245', NULL),
  ('CGI Alaska Juneau','175 S Franklin St, STE 305, Juneau, AK 99801', '+1 907-463-4112 x245', NULL),
  ('CGI Alaska Juneau','175 S Franklin ST., Unit 305, Juneau, AK 99801', '+1 907-463-4112 x245', NULL),
  ('CGI Arizona Tucson','7333 E Rosewood ST, Tucson, AZ 85710', '+1 520-733-8060', '+1 520-733-8096'),
  ('CGI Arizona Tucson','7333 E Rosewood Street, Tucson, AZ 85710', '+1 520-733-8060', '+1 520-733-8096'),
  ('CGI Arizona Tucson','7333 E Rosewood Saint St, Tucson, AZ 85710', '+1 520-733-8060', '+1 520-733-8096'),
  ('CGI Arkansas Hot Springs','191 Office Park DR, Hot Springs, AR 71913', '+1 501-622-2440', '+1 501-622-2442'),
  ('CGI Arkansas Hot Springs','191 Office Park Drive, Hot Springs, AR 71913', '+1 501-622-2440', '+1 501-622-2442'),
  ('CGI Arkansas Hot Springs','191 Office Park DR., Unit 2440, Hot Springs, AR 71913', '+1 501-622-2440', '+1 501-622-2442'),
  ('CGI California Los Angeles','350 South Grand Avenue, Suite 3570, Los Angeles, CA 90071', '+1 213-239-5300', '+1 213-239-5301'),
  ('CGI California Los Angeles','350 S Grand Ave, STE 3570, Los Angeles, CA 90071', '+1 213-239-5300', '+1 213-239-5301'),
  ('CGI California Los Angeles','350 S Grand AVE., Unit 3570, Los Angeles, CA 90071', '+1 213-239-5300', '+1 213-239-5301'),
  ('CGI California Sacramento','400 Capitol Mall, Suite 1500, Sacramento, CA 95814', '+1 916-830-1100', NULL),
  ('CGI California Sacramento','400 Capitol Mall, STE 1500, Sacramento, CA 95814', '+1 916-830-1100', NULL),
  ('CGI California Sacramento','400 Capitol Mall, Unit 1500, Sacramento, CA 95814', '+1 916-830-1100', NULL),
  ('CGI California San Diego','2878 Camino Del Rio South, Suite 500, San Diego, CA 92108', '+1 619-725-8400', '+1 619-725-8401'),
  ('CGI California San Diego','2878 Camino Del Rio S, STE 500, San Diego, CA 92108', '+1 619-725-8400', '+1 619-725-8401'),
  ('CGI California San Diego','2878 Camino Del Rio South, Unit 500, San Diego, CA 92108', '+1 619-725-8400', '+1 619-725-8401'),
  ('CGI California San Francisco','345 California Street, Suite 600 & 700, San Francisco, CA 94104', NULL, NULL),
  ('CGI California San Francisco','345 California St, STE 600 & 700, San Francisco, CA 94104', NULL, NULL),
  ('CGI California San Francisco','345 California ST., Unit 600 & 700, San Francisco, CA 94104', NULL, NULL),
  ('CGI California Walnut Creek','1550 Parkside Drive, Suite 150, Walnut Creek, CA 94596', NULL, NULL),
  ('CGI California Walnut Creek','1550 Parkside Dr., STE 150, Walnut Creek, CA 94596', NULL, NULL),
  ('CGI California Walnut Creek','1550 Parkside DR, Unit 150, Walnut Creek, CA 94596', NULL, NULL),
  -- Original records
  ('CGI Colorado Greenwood Village','5575 DTC Parkway, Suite 135, Greenwood Village, CO 80111', '+1 303-565-2201', NULL),
  ('CGI Connecticut Hartford','One American Row, Suite 403, Hartford, CT 06102-5056', '860-403-2040', '860-403-2041'),
  ('CGI Florida Jacksonville','1250 Imeson Park Blvd., Suite 504, Jacksonville, FL 32218', NULL, NULL),
  ('CGI Florida Miami Lakes','8100 Oak Lane, Suite 301, Miami Lakes, FL 33016', '305-817-2622', NULL),
  ('CGI Florida Orange Park','85 Industrial Loop N, Orange Park, FL 32073', NULL, NULL),
  ('CGI Florida Tampa','4300 West Cypress, Suite 300, Tampa, FL 33607', NULL, NULL),
  ('CGI Georgia Atlanta','945 East Paces Ferry Road, Suite 2600, Atlanta, GA 30326', '404-995-5000', NULL),
  ('CGI Hawaii Honolulu','715 South King Street, Suite 200, Honolulu, HI 96813', '+1 808-375-0999', NULL),
  ('CGI Illinois Oakbrook Terrace','1 Tower Ln, Suite 200, Oakbrook Terrace, IL 60181', NULL, NULL),
  ('CGI Kentucky Frankfort','229 West Main Street, Suite 204, Frankfort, KY 40601', '+1 614-865-5000', NULL),
  ('CGI Louisiana Lafayette','538 Cajundome Boulevard, Lafayette, LA 70506', '+1 337-484-1699', NULL),
  ('CGI Louisiana Lafayette','102 Versailles Boulevard, Lafayette, LA 70501', '+1 337-484-1650', NULL),
  ('CGI Maryland Baltimore','3120 Lord Baltimore Drive, Baltimore, MD 21244', '+1 410-597-4200', '+1 410-448-4787'),
  ('CGI Massachusetts Boston','100 Summer Street, Suite 1600, Boston, MA 02110', NULL, NULL),
  ('CGI Massachusetts Burlington','78 Blanchard Road, Suite 300, Burlington, MA 01803', '+1 781-565-2600', '+1 781-535-2700'),
  ('CGI Minnesota Bloomington','8500 Normandale Lake Blvd, Suite 1400, Bloomington, MN 55437', NULL, NULL),
  ('CGI Missouri Creve Coeur St. Louis','3 Cityplace Drive, 11th Floor, Creve Coeur, MO 63141', '314-216-2100', NULL),
  ('CGI Nevada Henderson','303 Water Street, Suite 200, Henderson, NV 89015', NULL, NULL),
  ('CGI Nevada Reno','1 East Liberty Street, Suite 601, Reno, NV 89501', NULL, NULL),
  ('CGI New Jersey New Brunswick','120 Albany Street, 7th Floor, New Brunswick, NJ 08901', '732-509-5660', NULL),
  ('CGI New York Latham Albany','3 Lear Jet Lane, Suite 205, Latham, NY 12110', NULL, NULL),
  ('CGI New York New York','115 Broadway, Suite 1501, New York, NY 10006', NULL, NULL),
  ('CGI New York New York','80 Broad Street, 5th Floor, Suite 503, New York, NY 10004', NULL, NULL),
  ('CGI New York New York','33 Whitehall Street, 15th Floor, New York, NY 10004', '+1 212-612-3600', '+1 212-612-3636'),
  ('CGI North Carolina Cary Raleigh','5000 CentreGreen Way, Suite 175, Cary, NC 27513', NULL, NULL),
  ('CGI North Carolina Charlotte','3426 Toringdon Way, Suite 405, Charlotte, NC 28277-3496', '1 704-409-3760', NULL),
  ('CGI Ohio Cleveland','1100 Superior Avenue East, Suite 200, Cleveland, OH 44114', '+1 216-687-1480', '+1 216-687-1488'),
  ('CGI Ohio Columbus','107 South High Street, Suite 200, Columbus, OH 43215', NULL, NULL),
  ('CGI Ohio Independence','6133 Rockside Road, Building II, Suite 302, Independence, OH 44131', '(216) 617-2989', NULL),
  ('CGI Ohio Westerville Columbus','570 Polaris Parkway, Suite 200, Westerville, OH 43082', '+1 614-865-5000', NULL),
  ('CGI Oklahoma Lawton','3120 NW Atlanta Ave, Lawton, OK 73505', NULL, NULL),
  ('CGI Pennsylvania Pittsburgh','611 William Penn Place, Suite 1200, Pittsburgh, PA 15219', '+1 412-258-3300', NULL),
  ('CGI Pennsylvania Plymouth Meeting','401 Plymouth Road, Suite 160, Plymouth Meeting, PA 19462', '+1 610-832-8110', '+1 610-832-8115'),
  ('CGI Puerto Rico Guaynabo San Juan','B7 Tabonuco Street, Suite 401, Guaynabo, PR 00968', '+1 787-625-9050', '+1 787-625-9060'),
  ('CGI South Carolina Columbia','250 Berryhill Road, Suite 205, Columbia, SC 29210', NULL, NULL),
  ('CGI South Carolina North Charleston','1101 Remount Road, Suite 300, North Charleston, SC 29406', '+1 843-745-9496', '+1 843-308-7191'),
  ('CGI Tennessee Chattanooga','1110 Market Street, Suite 418, Chattanooga, TN 37402', NULL, NULL),
  ('CGI Tennessee Franklin','6640 Carothers Pkwy, Suite 400, Franklin, TN 37067', '+1 615-224-0441', NULL),
  ('CGI Tennessee Knoxville','CGI Knoxville, Riverview Tower, 900 S Gay Street, Suite 1000, Knoxville, TN 37902', NULL, NULL),
  ('CGI Tennessee Nashville','2630 Elm Hill Pike, Suite 115, Nashville, TN 37214', NULL, NULL),
  ('CGI Texas Austin','2500 Bee Caves Road, Building III, Suite 100, Austin, TX 78746', '+1 512-306-7500', NULL),
  ('CGI Texas Austin','5113 Southwest Parkway, Suite 230, Austin, TX 78735', NULL, NULL),
  ('CGI Texas Belton','2100 Digby Drive, Belton, TX 76513', '+1 254-742-4050', '+1 254-774-4430'),
  ('CGI Texas Dallas','14800 Landmark Boulevard, Suite 300, Dallas, TX 75254', '+1 972-788-0400', '+1 972-788-0502'),
  ('CGI Texas Fort Worth','801 Cherry Street, Suite 1155, Fort Worth, TX 76102', '+1 817-332-7761', '+1 817-348-3613'),
  ('CGI Texas Houston','3700 West Sam Houston Parkway South, Suite 575, Houston, TX 77042', '+1 713-954-7000', '+1 713-954-7311'),
  ('CGI Texas San Antonio','5555 Northwest Parkway, Suite 143, San Antonio, TX 78249', NULL, NULL),
  ('CGI Utah Lehi','2701 North Thanksgiving Way, Suite 100, Lehi, UT 84043', '385-505-2826', NULL),
  ('CGI Virginia Arlington','1000 North Glebe Road, Arlington, VA 22201', NULL, NULL),
  ('CGI Virginia Fairfax','12601 Fair Lakes Circle, Fairfax, VA 22033', '+1 703-227-6000', '+1 703-227-6704'),
  ('CGI Virginia Fairfax','12601 Fair Lakes Cir, Fairfax, VA 22033', '+1 703-227-6000', '+1 703-227-6704'),
  ('CGI Virginia Fairfax','12601 Fair Lakes CIR., Fairfax, VA 22033', '+1 703-227-6000', '+1 703-227-6704'),
  ('CGI Virginia Fairfax','11325 Random Hills Road, 8th Floor, Fairfax, VA 22030', '+1 703-267-8000', '+1 703-267-5111'),
  ('CGI Virginia Fairfax','11325 Random Hills Rd, 8th Floor, Fairfax, VA 22030', '+1 703-267-8000', '+1 703-267-5111'),
  ('CGI Virginia Fairfax','11325 Random Hills RD., 8th Floor, Fairfax, VA 22030', '+1 703-267-8000', '+1 703-267-5111'),
  ('CGI Virginia Fairfax','11325 Random Hills RD., Eighth Floor, Fairfax, VA 22030', '+1 703-267-8000', '+1 703-267-5111'),
  ('CGI Virginia Lebanon','295 Technology Park Drive, Lebanon, VA 24266', '+1 276-889-7500', '+1 276-889-8507'),
  ('CGI Virginia Richmond','1111 East Main Street, Suite 615, Richmond, VA 23219', '+1 804-799-6600', NULL),
  ('CGI Washington Bellevue','500 108th Avenue NE, Suite 1100, Bellevue, WA 98004', '+1 425-943-3817', '+1 425-943-6801'),
  ('CGI Wisconsin Madison','25 West Main Street, 5th Floor, Madison, WI 53703', NULL, NULL),
  ('CGI Wisconsin Wausau','500 North 3rd Street, Suite 210, Wausau, WI 54403', '+1 715-393-4900', '+1 715-393-4701')
AS DATA(company, address, phone, fax);
select * from raw_data


-- COMMAND ----------

-- DBTITLE 1,Raw data
select * from raw_data

-- COMMAND ----------

-- DBTITLE 1,prompt worked - llama
CREATE OR REPLACE TEMPORARY VIEW proccessing_work AS
SELECT
  company,
from_json(
  ai_query(
    'databricks-llama-4-maverick',
    concat(
      'You are an address-standardization engine. Return ONLY valid JSON with the following fields:

- standardized_address (string or null)
  FORMAT: [Street Number] [Street Name], [Unit (if any)], [Line3 (if any)], [City], [State], [ZIP (5‑digit, no extension)]
- ai_confidence_score (float 0.0–1.0)
- ai_remark (short string)

Address Standardization Rules:
• Normalize and standardize street names, suffixes, and directionals (e.g., Street → St, Avenue → Ave).
• Normalize unit designators:
  - Suite → STE
  - Unit → UNIT
  - Apartment → APT
  - Floor → FL
  - Convert ordinal floors: “First Floor” → “1st FL”, “Second Floor” → “2nd FL”, etc.
• Correct casing to proper postal capitalization.
• Do NOT hallucinate missing information. Only return what is explicitly present and valid.
• If the input cannot be interpreted as a valid postal address, set:
    - standardized_address = null
    - ai_confidence_score = 0.0
• Keep ai_remark concise and factual.

Example:
Input: 12601 Fair Lakes Cir, Fairfax, VA 22033
Output: {"standardized_address": "12601 Fair Lakes Cir, Fairfax, VA 22033", "ai_confidence_score": 1.0, "ai_remark": "Address is valid."}

USER INPUT: ',
      address
    )
  ),
  'standardized_address STRING, ai_confidence_score DOUBLE, ai_remark STRING'
) AS ai_result_struct,
address as raw_address,
UPPER(ai_result_struct.standardized_address) AS standardized_address,
ai_result_struct.ai_confidence_score,
ai_result_struct.ai_remark,
  regexp_replace(phone, '[^0-9]', '') AS phone
FROM raw_data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Results: Standardized Addresses
-- MAGIC
-- MAGIC The table below shows how Databricks Generative AI standardizes different address variations to a single canonical form for ETL data cleansing.

-- COMMAND ----------

-- DBTITLE 1,Processing work table overview
select * from proccessing_work

-- COMMAND ----------

-- DBTITLE 1,Unique Company Contact Details Extraction
select distinct company, standardized_address from proccessing_work

-- COMMAND ----------

-- DBTITLE 1,Distinct Company and Standardized Address Listing
select distinct
  company,
  standardized_address
from proccessing_work
qualify row_number() over (partition by 
   regexp_replace(
    standardized_address,
    ', (STE|SUITE|UNIT|APT|FL|FLOOR)', ',#'
  )
  order by standardized_address) = 1