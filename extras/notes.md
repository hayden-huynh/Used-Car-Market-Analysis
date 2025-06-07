# Possible transform operations
1. Remove records if missing any of these attributes: VIN, price, make, model, year, mileage

2. Enrich data:
    - actual vs expected price difference percentage
    - mileage per year
    - engine:
        - full name
        - cylinderCount
        - size
        - configuration
        - fuel grade
        - horsepower
        - torque
        - horsepowerRPM
        - torqueRPM
    - transmission:
        - transmissionType
        - automaticType
        - automaticSpeedCount
    - msrp:
        - baseMSRP
        - destinationCharge
    - mpg if missing:
        - highway
        - city
        - combined = .55 * city + .45 * highway

3. Process car option list: many-to-many relationship