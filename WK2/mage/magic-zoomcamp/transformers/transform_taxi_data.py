if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import pandas as pd

def remove_trips_with_zero_pax(data, *args, **kwargs):
    # Remove rows where the passenger count is equal to 0
    
    zero_pax_df = data[data['passenger_count'].isin([0])]
    zero_pax_count = zero_pax_df['passenger_count'].count()
    non_zero_pax_df = data[data['passenger_count'] > 0]
    non_zero_pax_count = non_zero_pax_df['passenger_count'].count()
    print(f'Preprocessing: records with zero passengers: {zero_pax_count}')
    print(f'Preprocessing: records with 1 passenger or more: {non_zero_pax_count}')

    return non_zero_pax_df

def remove_trips_with_zero_trip_distance(data, *args, **kwargs):
    # Remove rows where the trip distance is equal to zero.
    zero_trip_distance_df = data[data['trip_distance'].isin([0])]
    zero_trip_distance_count = zero_trip_distance_df['trip_distance'].count()
    non_zero_trip_distance_df = data[data['trip_distance'] > 0]
    non_zero_trip_distance_count = non_zero_trip_distance_df['trip_distance'].count()
    print(f'Preprocessing: records with zero trip distance: {zero_trip_distance_count}')
    print(f'Preprocessing: records with 1 trip distance or more: {non_zero_trip_distance_count}')

    return non_zero_trip_distance_df

@transformer
def transform(data, *args, **kwargs):
    # Specify your transformation logic here

    non_zero_pax_df = remove_trips_with_zero_pax(data, args, kwargs)

    final_df = remove_trips_with_zero_trip_distance(non_zero_pax_df, args, kwargs)

    # Create a new column `lpep_pickup_date` by converting `lpep_pickup_datetime` to a date.
    final_df['lpep_pickup_date'] = final_df['lpep_pickup_datetime'].dt.date
    final_df['lpep_dropoff_date'] = final_df['lpep_dropoff_datetime'].dt.date

    # Rename columns in Camel Case to Snake Case, e.g. `VendorID` to `vendor_id`.
    final_df.columns = final_df.columns.str.lower().str.replace(' ','_').str.replace('id','_id')

    print(f"vendor_id unique values: {final_df['vendor_id'].unique()}")
    return final_df


@test
def test_zero_passenger_count(output, *args) -> None:
    assert output['passenger_count'].isin([0]).sum() == 0, 'There are rides with zero passengers'

@test
def test_zero_trip_distance(output, *args) -> None:
    assert output['trip_distance'].isin([0]).sum() == 0, 'There are rides with zero trip distance'

@test
def test_vendor_id_1_or_2(output, *args) -> None:
    assert output['vendor_id'].isin(output['vendor_id'].unique()).sum() == output['vendor_id'].count(), 'There are ride entries with vendor_id that is no 1 or 2'