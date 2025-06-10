"""
Data Transformation Module for Insurance ETL Pipeline

This module handles business logic transformations for customer, policy, and claims data
to prepare data for loading into insurance star schema dimension and fact tables.
Includes data cleaning, standardization, and preparation for dimensional modeling.

STUDENT ASSIGNMENT: Complete the 5 TODO methods below to implement business logic

Key Components:
- InsuranceTransformer: Main transformation class for insurance operations
- Customer standardization: Name cleaning, age calculation, risk classification
- Policy enrichment: Premium tier calculation, coverage validation
- Claims processing: Amount validation, status verification
- Data quality validation and preparation for dimension loading

Target Audience: Beginner/Intermediate students
Complexity Level: MEDIUM (~200 lines of code, 5 TODOs for students)
"""

import re
import logging
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, date
from collections import defaultdict
from config import InsuranceETLConfig


class InsuranceTransformer:
    """
    Main data transformation class for Insurance ETL pipeline

    Handles business logic transformations to prepare data for dimensional
    data warehouse loading with proper insurance star schema structure.
    """

    def __init__(self, config: InsuranceETLConfig):
        """
        Initialize transformer with configuration

        Args:
            config: Insurance ETL configuration instance
        """
        self.config = config
        self.logger = logging.getLogger(__name__)

        # Precompile regex patterns for performance
        self.phone_pattern = re.compile(r"[^\d]")
        self.email_pattern = re.compile(
            r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
        )

    # =====================================================================
    # STUDENT TODO METHODS - COMPLETE THESE 5 METHODS
    # =====================================================================

    def _calculate_customer_age(self, birth_date: str) -> int:
        """
        TODO #1: Calculate customer age from birth_date

        EXAMPLES:
        - Input: "1985-03-15" → Output: 39 (as of 2024)
        - Input: "1990-12-25" → Output: 33 (as of 2024)
        - Input: "" → Output: 0 (invalid date)

        BUSINESS RULES:
        - Calculate age in years from birth_date to current date
        - Return integer age
        - If birth_date is invalid or empty, return 0

        HINTS:
        - Use datetime.strptime(birth_date, "%Y-%m-%d") to parse the date
        - Use date.today() to get current date
        - Account for whether birthday has occurred this year
        - Handle ValueError for invalid dates

        TESTING:
        You can test this method by running:
        python transform.py
        """
        # TODO: Your implementation here
        # Remove this pass statement and implement the method

        # INSTR-SC: Insert a defensive programming check for empty birth_date here.

        try:
            dob = datetime.strptime(birth_date, "%Y-%m-%d")
            now = date.today()
            # Has birthday happened yet?
            if now.month > dob.month:
                return now.year - dob.year
            elif now.month == dob.month and now.day >= dob.day:
                return now.year - dob.year
            else:
                return max(
                    now.year - dob.year - 1, 0
                )  # catch case where less than 1 year
        except ValueError:
            return 0

    def _classify_customer_risk(self, risk_score: float, age: int) -> str:
        """
        TODO #2: Classify customer risk level

        EXAMPLES:
        - risk_score=1.5, age=30 → "Low"
        - risk_score=3.2, age=22 → "High"
        - risk_score=2.5, age=35 → "Medium"
        - risk_score=1.8, age=45 → "Low"

        BUSINESS RULES:
        - risk_score < 2.0 AND age >= 25: "Low"
        - risk_score >= 3.0 OR age < 25: "High"
        - Everything else: "Medium"

        HINTS:
        - Use if/elif/else statements for the logic
        - Check the AND condition first (both must be true)
        - Check the OR condition second (either can be true)
        - Return "Medium" for all other cases

        TESTING:
        Test with various combinations of risk_score and age
        """
        # TODO: Your implementation here
        # Remove this pass statement and implement the method
        if risk_score < 2.0 and age >= 25:
            return "Low"
        elif risk_score >= 3.0 or age < 25:
            return "High"
        else:
            return "Medium"

    def _standardize_phone(self, phone: str) -> str:
        """
        TODO #3: Standardize phone number format

        EXAMPLES:
        - Input: "555-123-4567" → Output: "(555) 123-4567"
        - Input: "" → Output: "UNKNOWN"
        - Input: "5551234567" → Output: "(555) 123-4567"
        - Input: "1-555-123-4567" → Output: "(555) 123-4567"
        - Input: "123" → Output: "INVALID"

        BUSINESS RULES:
        - If phone is missing/empty: return "UNKNOWN"
        - If phone has exactly 10 digits: format as (XXX) XXX-XXXX
        - If phone has 11 digits starting with 1: format as (XXX) XXX-XXXX (remove the 1)
        - Otherwise: return "INVALID"

        HINTS:
        - Use self.phone_pattern.sub("", phone) to extract only digits
        - Check the length of the digits string
        - Use string slicing to format: digits[:3], digits[3:6], digits[6:]
        - Handle the empty/None case first

        TESTING:
        Test with various phone formats from the sample data
        """
        # TODO: Your implementation here
        # Remove this pass statement and implement the method
        phone = self.phone_pattern.sub(
            "", phone
        )  # Uses regex pattern to strip non-digit characters
        if not phone:
            return "UNKNOWN"
        elif len(phone) == 10:
            return "(" + phone[:3] + ") " + phone[3:6] + "-" + phone[6:]
        elif len(phone) == 11:
            return "(" + phone[1:4] + ") " + phone[4:7] + "-" + phone[7:]
        else:
            return "INVALID"  # no pattern match but not empty

    def _determine_premium_tier(self, annual_premium: float) -> str:
        """
        TODO #4: Determine policy premium tier

        EXAMPLES:
        - annual_premium=2500 → "Premium"
        - annual_premium=1200 → "Standard"
        - annual_premium=800 → "Economy"
        - annual_premium=2000 → "Premium" (exactly 2000)

        BUSINESS RULES:
        - annual_premium >= 2000: "Premium"
        - annual_premium >= 1000: "Standard"
        - annual_premium < 1000: "Economy"

        HINTS:
        - Use if/elif/else statements in the correct order
        - Check the highest threshold first (>= 2000)
        - Then check the middle threshold (>= 1000)
        - The else case handles < 1000

        TESTING:
        Test with premium amounts around the boundary values (999, 1000, 1999, 2000)
        """
        # TODO: Your implementation here
        # Remove this pass statement and implement the method
        if annual_premium >= 2000:
            return "Premium"
        elif annual_premium >= 1000:
            return "Standard"
        else:
            return "Economy"

    def _validate_claim_amount(
        self, claim_amount: float, coverage_amount: float
    ) -> bool:
        """
        TODO #5: Validate claim amount against coverage

        EXAMPLES:
        - claim_amount=5000, coverage_amount=50000 → True
        - claim_amount=60000, coverage_amount=50000 → False
        - claim_amount=0, coverage_amount=50000 → False
        - claim_amount=50000, coverage_amount=50000 → True (exactly equal)

        BUSINESS RULES:
        - claim_amount must be > 0 (greater than zero)
        - claim_amount cannot exceed coverage_amount
        - Return True if valid, False if invalid

        HINTS:
        - Use logical AND operator: condition1 and condition2
        - First condition: claim_amount > 0
        - Second condition: claim_amount <= coverage_amount
        - Return the result of the combined condition

        TESTING:
        Test with edge cases: zero amounts, equal amounts, excessive amounts
        """
        # TODO: Your implementation here
        # Remove this pass statement and implement the method
        return (
            claim_amount > 0 and claim_amount <= coverage_amount
        )  # Returns true if both conditions met, false otherwise

    # =====================================================================
    # COMPLETED METHODS - DO NOT MODIFY BELOW THIS LINE
    # =====================================================================

    def prepare_customer_dimension(
        self, customers: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Transform and standardize customer data for dimension loading
        This method is complete - it calls your TODO methods above.
        """
        self.logger.info(
            f"Transforming customer dimension with {len(customers)} records"
        )

        transformed_customers = []

        for customer in customers:
            try:
                # Calculate age from birth_date (calls your TODO #1)
                age = self._calculate_customer_age(customer.get("birth_date", ""))

                # Clean and standardize names
                first_name, last_name, full_name = self._clean_customer_name(
                    customer.get("first_name", ""), customer.get("last_name", "")
                )

                # Standardize phone number (calls your TODO #3)
                phone = self._standardize_phone(customer.get("phone", ""))

                # Classify risk tier (calls your TODO #2)
                risk_score = float(customer.get("risk_score", 0))
                risk_tier = self._classify_customer_risk(risk_score, age)

                # Create transformed customer record
                transformed_customer = {
                    "customer_id": customer["customer_id"],
                    "first_name": first_name,
                    "last_name": last_name,
                    "full_name": full_name,
                    "email": customer.get("email", "").lower(),
                    "phone": phone,
                    "birth_date": customer.get("birth_date", ""),
                    "age": age,
                    "address": customer.get("address", ""),
                    "city": customer.get("city", ""),
                    "state": customer.get("state", ""),
                    "risk_score": risk_score,
                    "risk_tier": risk_tier,
                    "customer_since": customer.get("customer_since", ""),
                }

                transformed_customers.append(transformed_customer)

            except Exception as e:
                self.logger.warning(
                    f"Error transforming customer {customer.get('customer_id')}: {e}"
                )
                continue

        self.logger.info(f"Transformed {len(transformed_customers)} customer records")
        return transformed_customers

    def prepare_policy_dimension(
        self, policies: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Transform and enrich policy data for dimension loading
        This method is complete - it calls your TODO methods above.
        """
        self.logger.info(f"Transforming policy dimension with {len(policies)} records")

        transformed_policies = []

        for policy in policies:
            try:
                # Determine premium tier (calls your TODO #4)
                annual_premium = float(policy.get("annual_premium", 0))
                premium_tier = self._determine_premium_tier(annual_premium)

                # Validate and clean policy data
                coverage_amount = float(policy.get("coverage_amount", 0))
                deductible = float(policy.get("deductible", 0))

                # Create transformed policy record
                transformed_policy = {
                    "policy_id": policy["policy_id"],
                    "customer_id": policy["customer_id"],
                    "policy_type": policy.get("policy_type", "").title(),
                    "coverage_amount": coverage_amount,
                    "annual_premium": annual_premium,
                    "premium_tier": premium_tier,
                    "deductible": deductible,
                    "effective_date": policy.get("effective_date", ""),
                    "expiration_date": policy.get("expiration_date", ""),
                    "status": policy.get("status", "Active"),
                }

                transformed_policies.append(transformed_policy)

            except Exception as e:
                self.logger.warning(
                    f"Error transforming policy {policy.get('policy_id')}: {e}"
                )
                continue

        self.logger.info(f"Transformed {len(transformed_policies)} policy records")
        return transformed_policies

    def prepare_agent_dimension(
        self, claims: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Extract and prepare agent dimension data from claims
        Args:
            claims: List of claims records containing agent info
        Returns:
            List of unique agent dimension records
        """
        self.logger.info("Extracting agent dimension from claims data")
        # Extract unique agents from claims
        agents_dict = {}
        for claim in claims:
            agent_id = claim.get("agent_id")
            if agent_id and agent_id not in agents_dict:
                # Create basic agent record
                # Parse agent name from ID if format allows
                agent_parts = agent_id.replace("AGT", "Agent ").split()
                agent = {
                    "agent_id": agent_id,
                    "first_name": agent_parts[0] if agent_parts else "Agent",
                    "last_name": agent_parts[1] if len(agent_parts) > 1 else agent_id,
                    "full_name": f"Agent {agent_id}",
                    "region": self._assign_agent_region(agent_id),
                    "experience_years": self._estimate_experience_years(),
                    "hire_date": None,  # Could be populated from HR system
                }
                agents_dict[agent_id] = agent
        agents = list(agents_dict.values())
        self.logger.info(f"Extracted {len(agents)} unique agents from claims")
        return agents

    def prepare_date_dimension_keys(self, claims: List[Dict[str, Any]]) -> List[int]:
        """
        Extract and format date keys from claims data
        Args:
            claims: List of claims records
        Returns:
            List of unique date keys in YYYYMMDD format
        """
        date_keys = set()
        for claim in claims:
            # Process filed_date
            filed_date = claim.get("filed_date")
            if filed_date:
                date_key = self._generate_date_key(filed_date)
                if date_key > 0:
                    date_keys.add(date_key)
            # Process closed_date if available
            closed_date = claim.get("closed_date")
            if closed_date:
                date_key = self._generate_date_key(closed_date)
                if date_key > 0:
                    date_keys.add(date_key)
        sorted_keys = sorted(list(date_keys))
        self.logger.info(f"Generated {len(sorted_keys)} unique date keys")
        return sorted_keys

    def prepare_claims_facts(
        self, claims: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Transform claims data for fact table loading
        This method is complete - it calls your TODO methods above.
        """
        self.logger.info(f"Transforming claims facts with {len(claims)} records")

        transformed_facts = []

        for claim in claims:
            try:
                # Validate claim amount against coverage (calls your TODO #5)
                claim_amount = float(claim.get("claim_amount", 0))
                coverage_amount = float(claim.get("coverage_amount", 0))

                if not self._validate_claim_amount(claim_amount, coverage_amount):
                    self.logger.warning(
                        f"Invalid claim amount for {claim.get('claim_id')}"
                    )
                    continue

                # Generate date keys
                filed_date_key = self._generate_date_key(claim.get("filed_date"))
                closed_date_key = (
                    self._generate_date_key(claim.get("closed_date"))
                    if claim.get("closed_date")
                    else None
                )

                # Calculate processing days
                processing_days = int(claim.get("processing_days", 0))

                # Create fact record
                fact_record = {
                    "claim_id": claim["claim_id"],
                    "policy_id": claim["policy_id"],
                    "customer_id": claim["customer_id"],
                    "agent_id": claim["agent_id"],
                    "filed_date_key": filed_date_key,
                    "closed_date_key": closed_date_key,
                    "claim_amount": claim_amount,
                    "coverage_amount": coverage_amount,
                    "deductible_amount": float(claim.get("deductible_amount", 0)),
                    "payout_amount": float(claim.get("payout_amount", 0)),
                    "processing_days": processing_days,
                    "claim_status": claim.get("claim_status", ""),
                }

                transformed_facts.append(fact_record)

            except Exception as e:
                self.logger.warning(
                    f"Error transforming claim {claim.get('claim_id')}: {e}"
                )
                continue

        self.logger.info(f"Transformed {len(transformed_facts)} claims fact records")
        return transformed_facts

    # =====================================================================
    # HELPER METHODS - ALREADY IMPLEMENTED
    # =====================================================================

    def _clean_customer_name(
        self, first_name: str, last_name: str
    ) -> Tuple[str, str, str]:
        """
        Clean and standardize customer names

        Args:
            first_name: Raw first name
            last_name: Raw last name

        Returns:
            Tuple of (cleaned_first_name, cleaned_last_name, full_name)
        """
        # Clean and apply basic title case
        first_clean = first_name.strip().title() if first_name else ""
        last_clean = last_name.strip().title() if last_name else ""

        # Handle special name cases (apostrophes, hyphens, etc.)
        first_clean = self._fix_name_casing(first_clean)
        last_clean = self._fix_name_casing(last_clean)

        # Create full name
        full_name = f"{first_clean} {last_clean}".strip()

        return first_clean, last_clean, full_name

    def _fix_name_casing(self, name: str) -> str:
        """Fix common name casing issues"""
        if not name:
            return name

        # Handle names with apostrophes (O'Brien, D'Angelo)
        if "'" in name:
            parts = name.split("'")
            name = "'".join([part.capitalize() for part in parts])

        # Handle names with hyphens (Mary-Jane, Jean-Luc)
        if "-" in name:
            parts = name.split("-")
            name = "-".join([part.capitalize() for part in parts])

        return name

    def _assign_agent_region(self, agent_id: str) -> str:
        """
        Assign agent region based on agent ID

        Args:
            agent_id: Agent identifier

        Returns:
            str: Assigned region name
        """
        # Simple region assignment based on agent ID
        agent_num = int(agent_id.replace("AGT", "")) if "AGT" in agent_id else 0

        if agent_num <= 5:
            return "North"
        elif agent_num <= 10:
            return "South"
        else:
            return "Central"

    def _estimate_experience_years(self) -> int:
        """Estimate agent experience years (random for demo)"""
        import random

        return random.randint(1, 15)

    def _generate_date_key(self, date_value: Any) -> int:
        """
        Generate date key in YYYYMMDD format

        Args:
            date_value: Date value (string or datetime)

        Returns:
            int: Date key in YYYYMMDD format
        """
        if not date_value:
            return 0

        try:
            if isinstance(date_value, str):
                dt = datetime.strptime(date_value, "%Y-%m-%d")
                return int(dt.strftime("%Y%m%d"))
            elif isinstance(date_value, (datetime, date)):
                return int(date_value.strftime("%Y%m%d"))
            else:
                return 0
        except (ValueError, AttributeError):
            return 0

    def transform_all_for_insurance_schema(
        self, raw_data: Dict[str, List[Dict[str, Any]]]
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Transform all data sources for insurance star schema loading

        Args:
            raw_data: Dictionary containing raw data from all sources

        Returns:
            Dictionary containing transformed data ready for dimension and fact loading
        """
        self.logger.info("Starting coordinated transformation for insurance schema")
        transformed_data = {}

        # Transform customer dimension data
        customers = raw_data.get("customers", [])
        transformed_data["dim_customer"] = self.prepare_customer_dimension(customers)

        # Transform policy dimension data
        policies = raw_data.get("policies", [])
        transformed_data["dim_policy"] = self.prepare_policy_dimension(policies)

        # Transform claims data for facts and agent dimension
        claims = raw_data.get("claims", [])
        transformed_data["dim_agent"] = self.prepare_agent_dimension(claims)
        transformed_data["fact_claims"] = self.prepare_claims_facts(claims)
        transformed_data["date_keys"] = self.prepare_date_dimension_keys(claims)

        # Log transformation summary
        self.logger.info("Transformation Summary:")
        for component, data in transformed_data.items():
            if component != "date_keys":
                self.logger.info(f"  - {component}: {len(data)} records")
            else:
                self.logger.info(f"  - {component}: {len(data)} unique dates")

        return transformed_data


def transform_for_insurance_schema(
    config: InsuranceETLConfig, raw_data: Dict[str, List[Dict[str, Any]]]
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Convenience function to transform all data for insurance schema

    Args:
        config: Insurance ETL configuration
        raw_data: Raw data from extraction phase

    Returns:
        Dictionary containing transformed data for all insurance schema components
    """
    transformer = InsuranceTransformer(config)
    return transformer.transform_all_for_insurance_schema(raw_data)


if __name__ == "__main__":
    # Test transformation functionality
    from config import load_config

    try:
        config = load_config()
        transformer = InsuranceTransformer(config)

        # Test with sample data
        sample_customers = [
            {
                "customer_id": "CUST001",
                "first_name": "john",
                "last_name": "SMITH",
                "email": "John.Smith@Example.COM",
                "phone": "555-123-4567",
                "birth_date": "1985-03-15",
                "risk_score": 2.3,
            }
        ]

        print("Testing insurance transformation...")
        transformed_customers = transformer.prepare_customer_dimension(sample_customers)

        print("Transformation Results:")
        print(f"- Customers: {len(transformed_customers)} records")
        if transformed_customers:
            customer = transformed_customers[0]
            print(f"  - Age: {customer['age']}")
            print(f"  - Risk Tier: {customer['risk_tier']}")
            print(f"  - Phone: {customer['phone']}")

        # INSTR Manual test: calculate age
        age_test = transformer._calculate_customer_age("1990-01-01")
        print(f"Age test: {age_test} (should be ~34)")

        # INSTR Manual test: classify risk score
        risk_low = transformer._classify_customer_risk(1.5, 30)  # Should be "Low"
        risk_high = transformer._classify_customer_risk(3.2, 22)  # Should be "High"
        risk_med = transformer._classify_customer_risk(2.5, 35)  # Should be "Medium"
        print(f"Risk tests: {risk_low}, {risk_high}, {risk_med}")

        # INSTR Manual test: standardize phone number
        phone1 = transformer._standardize_phone(
            "555-123-4567"
        )  # Should be "(555) 123-4567"
        phone2 = transformer._standardize_phone("")  # Should be "UNKNOWN"
        phone3 = transformer._standardize_phone(
            "5551234567"
        )  # Should be "(555) 123-4567"
        print(f"Phone tests: {phone1}, {phone2}, {phone3}")

        # INSTR Manual test: determine premium category
        tier1 = transformer._determine_premium_tier(2500)  # Should be "Premium"
        tier2 = transformer._determine_premium_tier(1200)  # Should be "Standard"
        tier3 = transformer._determine_premium_tier(800)  # Should be "Economy"
        print(f"Premium tests: {tier1}, {tier2}, {tier3}")

        # INSTR Manual test: validate claim amount
        valid1 = transformer._validate_claim_amount(5000, 50000)  # Should be True
        valid2 = transformer._validate_claim_amount(60000, 50000)  # Should be False
        valid3 = transformer._validate_claim_amount(0, 50000)  # Should be False
        print(f"Validation tests: {valid1}, {valid2}, {valid3}")

    except Exception as e:
        print(f"Transformation test failed: {e}")
