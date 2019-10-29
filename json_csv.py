import json
import csv
import ast
from tqdm import tqdm


def parse_data(source1, output_path):
    # setup an array for writing each row in the csv file
    rows = []
    # extract fields from business json data set #
    # setup an array for storing each json entry
    business_data = []
    # setup an array for headers we are not using strictly
    business_header_removals = ['address', 'city', 'latitude', 'longitude', 'name', 'postal_code', 'review_count', 'stars',
                                'state','attributes', 'categories', 'hours', 'is_open']
    # setup an array for headers we are adding
    business_header_additions = ['AcceptsInsurance','ByAppointmentOnly','BusinessAcceptsCreditCards','BusinessParking_garage',
                                 'BusinessParking_street','BusinessParking_validated','BusinessParking_lot','BusinessParking_valet',
                                 'HairSpecializesIn_coloring','HairSpecializesIn_africanamerican','HairSpecializesIn_curly','HairSpecializesIn_perms',
                                 'HairSpecializesIn_kids','HairSpecializesIn_extensions','HairSpecializesIn_asian','HairSpecializesIn_straightperms',
                                 'RestaurantsPriceRange2','GoodForKids','WheelchairAccessible','BikeParking','Alcohol','HasTV','NoiseLevel',
                                 'RestaurantsAttire','Music_dj','Music_background_music','Music_no_music','Music_karaoke','Music_live','Music_video',
                                 'Music_jukebox','Ambience_romantic','Ambience_intimate','Ambience_classy','Ambience_hipster','Ambience_divey',
                                 'Ambience_touristy','Ambience_trendy','Ambience_upscale','Ambience_casual','RestaurantsGoodForGroups,Caters',
                                 'WiFi','RestaurantsReservations','RestaurantsTakeOut','HappyHour','GoodForDancing','RestaurantsTableService',
                                 'OutdoorSeating','RestaurantsDelivery','BestNights_monday','BestNights_tuesday','BestNights_friday','BestNights_wednesday',
                                 'BestNights_thursday','BestNights_sunday','BestNights_saturday','GoodForMeal_dessert,GoodForMeal_latenight',
                                 'GoodForMeal_lunch','GoodForMeal_dinner','GoodForMeal_breakfast','GoodForMeal_brunch','CoatCheck','Smoking','DriveThru',
                                 'DogsAllowed','BusinessAcceptsBitcoin','Open24Hours','BYOBCorkage','BYOB','Corkage','DietaryRestrictions_dairy-free',
                                 'DietaryRestrictions_gluten-free','DietaryRestrictions_vegan','DietaryRestrictions_kosher','DietaryRestrictions_halal',
                                 'DietaryRestrictions_soy-free','DietaryRestrictions_vegetarian','AgesAllowed','RestaurantsCounterService']
    # open the business source file
    with open(source1, encoding="utf8") as f:
        # for each line in the json file
        for line in f:
            # store the line in the array for manipulation
            business_data.append(json.loads(line))
    # close the reader
    f.close()
    # append the initial keys as csv headers
    header = sorted(business_data[0].keys())
    # remove keys from the business data that we are not using strictly
    for headers in business_header_removals:
        header.remove(headers)

    # append the additional business related csv headers
    for headers in business_header_additions:
        header.append(headers)
    print('processing data in the business dataset...')
    # for every entry in the business data array

    for entry in tqdm(range(0, len(business_data))):
        row = []
        row.append(business_data[entry]['business_id'])
        # for each attribute that is not nested
        for attribute in business_header_additions:
            attr = attribute.split('_')
            if(len(attr) == 1):
                # if there is an attribute
                if business_data[entry]['attributes'] is not None and attribute in business_data[entry]['attributes']:
                    # if the attribute contains true
                    if business_data[entry]['attributes'][attribute] is True:
                        row.append(1)
                    # else if the attribute contains false
                    elif business_data[entry]['attributes'][attribute] is False:
                        row.append(0)
                    # else if the attribute is non-empty and not true of false
                    elif business_data[entry]['attributes'][attribute] is not None:
                        row.append(business_data[entry]['attributes'][attribute])
                        # print(business_data[entry]['attributes'][attribute])
                # else of the attribute is not available
                else:
                    # append NA for the attribute
                    row.append('NA')
            elif(len(attr) == 2):
                # if there is an attribute
                if business_data[entry]['attributes'] is not None and attr[0] in business_data[entry]['attributes']:
                    # convert string to dict
                    k = ast.literal_eval(business_data[entry]['attributes'][attr[0]]) 

                    if k and attr[1] in k:
                        # if the attribute contains true
                        if k[attr[1]] is True:
                            row.append(1)
                        # else if the attribute contains false
                        elif k[attr[1]] is False:
                            row.append(0)
                        # else if the attribute is non-empty and not true of false
                        elif k[attr[1]] is not None:
                            row.append(k[attr[1]])
                            print(k[attr[1]])
                    else:
                        row.append('NA')
                # else of the attribute is not available
                else:
                    # append NA for the attribute
                    row.append('NA')

        # remove stray text, such as "\n" form address
        # set up an array for the cleaned row entries
        row_clean = []
        # for every item in the row
        for item in row:
            # scan and replace for nasty text
            row_clean.append(str(item).replace('\n', ' '))
        # after all fields have been extracted and cleaned, append the row to the rows array for writing to csv
        rows.append(row_clean)

    # write to csv file
    # print(header)
    with open(output_path, 'w') as out:
        writer = csv.writer(out)
        # write the csv headers
        writer.writerow(header)
        # for each entry in the row array
        print('writing contents to csv...')
        for entry in tqdm(range(0, len(rows))):
            try:
                # write the row to the csv
                writer.writerow(rows[entry])
            # if there is an error, continue to the next row
            except UnicodeEncodeError:
                continue
    out.close()

parse_data('yelp_academic_dataset_business.json',
           'yelp_academic_dataset_business.csv')