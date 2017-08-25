"""
    AUTHOR:
        Yat Long Lo

    EMAIL:
        yllo2@illinois.edu
        
    PROGRAM DESCRIPTION:
        This program is a prototype, as an attempt to perform image check between results of BasicFusion and the original data 
        files. 3 major attempts have been made:
            
            1. Data from BasicFusion files and original data files are compared using structural similarity index (SSI). This is done by converting
               the data to grayscale image and calculate the corresponding SSIs. If the BasicFusion program is working corrently, the SSI should be close to 1,
               indicating the data are identical between BasicFusion and original data
            
            2. Finding MISR data blocks in MODIS data. This is again done by converting all the data to grayscale images. To perform matching, the SIFT algorithm
               is used which is scale-invariant. The matching is done by locating good keypoints between 2 images. The testing has been successful but a threshold has
               to be set, by possibly look at the average number of keypoints when matching one block of MISR to MODIS. For each matching, we choose a MISR block with the
               greatest contrast in the orbit to avoid possible errors or dark patches in the orbit.
            
            3. MPI has been added to speed up the performance. But it has not been fully tested due to the limitation of time. 1 MISR-MODIS matching normally takes around
               1.5-2.5 minutes. To improve the performance from days to hours in processing 1 year of data, parallelization has to be used. The current configuration is to use
               10 nodes and 160 cores on BlueWaters, which should take less than 4 hours for 1 year of data if the program works correctly.

"""

import sys
import os
import h5py
import cv2
import re
import numpy as np
from scipy import ndimage
from PIL import Image
from pyhdf.SD import SD, SDC
from skimage import data, img_as_float
from skimage.measure import compare_ssim as ssim
from mpi4py import MPI

FILL_VALUE = 65504
MIN_MATCH_COUNT = 10

#MPI variables for job range division
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

def misr_img_convert(rad_data, fill_value, name, blocks = None):
    """
                    misr_img_convert
    DESCRIPTION:
        This function converts a MISR data array from the original data file (HDF4) to the range of a grayscale image. All values that are greater than or
        equal to the fill_value are set to zero. User can specify the fill_value and blocks, which is a list of indices
        to indicate the blocks to be extracted. The converted data array is then saved as a grayscale image, named by the 
        variable name. The blocks data should retrieve consecutive blocks, or else, the image similarity matching processing 
        between MISR and MODIS would not work properly
        
    ARGUMENTS:
        0. rad_data -- This argument is the data array containing the MISR radiance data
        1. fill_value -- This argument corrsponds to the fill value of the data array
        2. name -- The name of the image to be saved
        3. blocks -- This variable specifies the indices of blocks to take from the MISR data. All blocks would be returned if it's not specified
        
    EFFECTS:
        The function would convert a data array from granules of MISR data to values of a grayscale image, and a grayscale image would be created and saved
        
    RETURN:
        The stitched MISR data would be return in the form of a numpy array
        
    """    
    rad_fill_index = rad_data >= fill_value
    rad_data[rad_fill_index] = 0.0
    max_val = np.amax(rad_data)
    print("Max value: " + str(max_val))
    rad_data = (rad_data / float(max_val)) * 255.99
    
    if(blocks == None):
        num_blocks = rad_data.shape[0]
        stitched_data = rad_data[0]
        
        for i in range(1,num_blocks):
            stitched_data = np.concatenate([stitched_data, rad_data[i]], axis=0)
    else:
        num_blocks = len(blocks)
        stitched_data = rad_data[blocks[0]]
        if(num_blocks > 1):
            for i in range(1, num_blocks):
                stitched_data = np.concatenate([stitched_data, rad_data[blocks[i]]], axis=0)
    
    print("Stitched data shape: " + str(stitched_data.shape))
    
    
    #For saving actual image
    img = Image.fromarray(np.uint8(stitched_data), "L")
    img.save(name)
    
    return np.uint8(stitched_data)


def misr_img_unit_convert(rad_data, fill_value, name):
    """
                    misr_img_unit_convert
    DESCRIPTION:
        This function converts a MISR data array from the original data file (HDF4) to the range of a grayscale image. All values that are greater than or
        equal to the fill_value are set to zero. User can specify the fill_value. The converted data array would then be used to get the block with the greatest
        contrast for image similarity check
        
    ARGUMENTS:
        0. rad_data -- This argument is the data array containing the MISR radiance data
        1. fill_value -- This argument corrsponds to the fill value of the data array
        2. name -- The name of the image to be saved
        
    EFFECTS:
        The function would convert a data array of a block with greatest contrast, and a grayscale image would be created and saved
        
    RETURN:
        The MISR block data would be return in the form of a numpy array
        
    """    
    rad_fill_index = rad_data >= fill_value
    rad_data[rad_fill_index] = 0.0
    max_val = np.amax(rad_data)
    print("Max value: " + str(max_val))
    rad_data = (rad_data / float(max_val)) * 255.99
    best_misr_index = misr_get_best_block(rad_data)
    block_data = np.array(rad_data[best_misr_index])
    
    
    #For saving actual image
    img = Image.fromarray(np.uint8(block_data), "L")
    img.save(name)
    
    return np.uint8(block_data)

def bf_misr_img_convert(rad_data, fill_value, name, blocks=None):
    """
                    bf_misr_img_convert
    DESCRIPTION:
        This function converts a MISR data array from the BasicFusion file (HDF5) to the range of a grayscale image. All values that are greater than or
        equal to the fill_value are set to zero. User can specify the fill_value and blocks, which is a list of indices
        to indicate the blocks to be extracted. The converted data array is then saved as a grayscale image, named by the 
        variable name. The blocks data should retrieve consecutive blocks, or else, the image similarity matching processing 
        between MISR and MODIS would not work properly
        
    ARGUMENTS:
        0. rad_data -- This argument is the data array containing the MISR radiance data
        1. fill_value -- This argument corrsponds to the fill value of the data array
        2. name -- The name of the image to be saved
        3. blocks -- This variable specifies the indices of blocks to take from the MISR data. All blocks would be returned if it's not specified
        
    EFFECTS:
        The function would convert a data array from granules of MISR data to values of a grayscale image, and a grayscale image would be created and saved
        
    RETURN:
        The stitched MISR data would be return in the form of a numpy array
        
    """    
    rad_fill_index = rad_data == fill_value
    rad_data[rad_fill_index] = 0.0
    max_val = np.amax(rad_data)
    print("Max value: " + str(max_val))
    rad_data = (rad_data / float(max_val)) * 255.99
    
    num_blocks = rad_data.shape[0]
    stitched_data = rad_data[0]
    
    if(blocks == None):
        num_blocks = rad_data.shape[0]
        stitched_data = rad_data[0]
        
        for i in range(1,num_blocks):
            stitched_data = np.concatenate([stitched_data, rad_data[i]], axis=0)
    else:
        num_blocks = len(blocks)
        stitched_data = rad_data[blocks[0]]
        if(num_blocks > 1):
            for i in range(1, num_blocks):
                stitched_data = np.concatenate([stitched_data, rad_data[blocks[i]-1]], axis=0)
    
    print("Stitched data shape: " + str(stitched_data.shape))
    print(stitched_data)
    
    #For saving actual image
    img = Image.fromarray(np.uint8(stitched_data), "L")
    img.save(name)
    
    return np.uint8(stitched_data)

def bf_modis_img_convert(rad_data, fill_value, name):
    
    """
                    bf_modis_img_convert
    DESCRIPTION:
        This function converts a MODIS data array from the BasicFusion file (HDF5) to the range of a grayscale image. All values that are greater than or
        equal to the fill_value are set to zero. User can specify the fill_value. The converted data array is then saved as a grayscale image, named by the 
        variable name.
        
    ARGUMENTS:
        0. rad_data -- This argument is the data array containing the MODIS radiance data
        1. fill_value -- This argument corrsponds to the fill value of the data array
        2. name -- The name of the image to be saved
        
    EFFECTS:
       The function would convert a data array from granules of MODIS data to values of a grayscale image, and a grayscale image would be created and saved
        
    RETURN:
        The stitched MODIS data would be return in the form of a numpy array
        
    """    
    rad_fill_index = rad_data == fill_value
    rad_data[rad_fill_index] = 0.0
    max_val = np.amax(rad_data)
    print("Max value: " + str(max_val))
    rad_data = (rad_data / float(max_val)) * 255.99
    
    img = Image.fromarray(np.uint8(rad_data), "L")
    img.save(name)
    
    return np.uint(rad_data)

def bf_misr_modis_match(misr_img, modis_img):
    """
                    bf_misr_modis_match
    DESCRIPTION:
        This test function takes in 2 path to 2 images, one for a MISR image and one for MODIS image. Image similarity check is performed, making use of the
        SIFT algorithm. If the number of good matches exceeds the minimum match count, they will be considered as a match. In this demo function, a combined
        image would be created to show the matched features/textures
        
    ARGUMENTS:
        0. misr_img -- A path to the MISR image
        1. modis_img -- A path to the MODIS image
        
    EFFECTS:
       The function reads in 2 images and uses SIFT to detect similarity. An image is created and saved if number of matches found is greater than the threshold
        
    RETURN:
        Nothing will be returned
        
    """  
    
    img1 = cv2.imread(misr_img, 0)
    img2 = cv2.imread(modis_img, 0)
    # Initiate SIFT detector
    #sift = cv2.xfeatures2d.SIFT()
    sift = cv2.xfeatures2d.SIFT_create()
    
    # find the keypoints and descriptors with SIFT
    kp1, des1 = sift.detectAndCompute(img1,None)
    kp2, des2 = sift.detectAndCompute(img2,None)
    
    FLANN_INDEX_KDTREE = 0
    index_params = dict(algorithm = FLANN_INDEX_KDTREE, trees = 5)
    search_params = dict(checks = 50)
    
    flann = cv2.FlannBasedMatcher(index_params, search_params)
    
    matches = flann.knnMatch(des1,des2,k=2)
    
    # store all the good matches as per Lowe's ratio test.
    good = []
    for m,n in matches:
        if m.distance < 0.7*n.distance:
            good.append(m)
    print("Number of good matches: " + str(len(good)))
    if len(good)> MIN_MATCH_COUNT:
        return len(good)
        #Commented out codes from drawing bounding boxes and feature mapping lines in a new image
        #src_pts = np.float32([ kp1[m.queryIdx].pt for m in good ]).reshape(-1,1,2)
        #dst_pts = np.float32([ kp2[m.trainIdx].pt for m in good ]).reshape(-1,1,2)
    
        #M, mask = cv2.findHomography(src_pts, dst_pts, cv2.RANSAC,5.0)
        #matchesMask = mask.ravel().tolist()
    
        #h,w = img1.shape
        #pts = np.float32([ [0,0],[0,h-1],[w-1,h-1],[w-1,0] ]).reshape(-1,1,2)
        #dst = cv2.perspectiveTransform(pts,M)
    
        #img2 = cv2.polylines(img2,[np.int32(dst)],True,255,3, cv2.LINE_AA)
    else:
        return len(good)
        print("Not enough matches are found - %d/%d" % (len(good),MIN_MATCH_COUNT))
        matchesMask = None
        return
    
    draw_params = dict(matchColor = (0,255,0), singlePointColor = None, matchesMask = matchesMask, flags = 2)
    img3 = cv2.drawMatches(img1,kp1,img2,kp2,good,None,**draw_params)
    cv2.imwrite("misr_modis_match2blocks.png", img3)
    print("Matching image created")
    return
    

def misr_get_best_block(dataset):
    """
                    misr_get_best_block
    DESCRIPTION:
        This function takes a MISR dataset and finds the block with the greatest contrast to use for feature mapping
        
    ARGUMENTS:
        0. dataset -- This variable holds values of a MISR radiance dataset
        
    EFFECTS:
        The function finds the best block for feature mapping, using contrast as a metric
        
    RETURN:
        The index of the best block (block with the greatest contrast) will be returned
        
    """    
    best_index = 0
    best_block = np.array(dataset[0])
    best_var = ndimage.variance(np.array(best_block))
    for i in range(1, 180):
        temp_var = ndimage.variance(np.array(dataset[i]))
        if(temp_var > best_var):
            best_index = i
            best_block = np.array(dataset[i])
            best_var = temp_var
    print("Block " + str(best_index) + " has the highest variance: " + str(best_var))
    return best_index

def misr_hdf4_read_dataset(src_filename, datasetname):
    """
                    misr_hdf4_read_dataset
    DESCRIPTION:
        This function reads a MISR dataset from an original MISR data file in the format of HDF4.
        
    ARGUMENTS:
        0. src_filename -- This argument is the filename of the original MISR data file, which is a HDF4 file
        1. datasetname -- This argument is the name of the dataset to be retrieved from the source file
        
    EFFECTS:
        The function would look for the dataset based on the datasetname after opening up the source file.
        
    RETURN:
        The dataset found would be returned
        
    """    
    print("file: " + src_filename)
    src_f = SD(src_filename, SDC.READ)
    src_obj = src_f.select(datasetname)
    misr_data = src_obj.get()
    
    src_f.end()
    #src_obj.endaccess()
    return misr_data

def misr_hdf5_read_dataset(src_filename, dataset_path):
    """
                    misr_hdf5_read_dataset
    DESCRIPTION:
        This function reads a MISR dataset from a BasicFusion file inf the format of HDF5.
        
    ARGUMENTS:
        0. src_filename -- This argument is the filename of the BasicFusion file, which is a HDF5 file
        1. dataset_path -- This argument is the path to the dataset to be retrieved within the BasicFusion file
        
    EFFECTS:
        The function would look for the dataset based on the dataset_path after opening up the source file.
        
    RETURN:
        The dataset found would be returned
        
    """    
    print("BF file: " + src_filename)
    bf_f = h5py.File(src_filename, "r")
    dataset = bf_f[dataset_path][()]
    return dataset

def modis_hdf5_read(src_filename):
    """
                    modis_hdf5_read
    DESCRIPTION:
        This function reads MODIS data from a BasicFusion file inf the format of HDF5. Checks are performed to only consider
        granules that has all the required resolutions. Then, valid granules are stitched together and returned.
        
    ARGUMENTS:
        0. src_filename -- This argument is the filename of the original MISR data file, which is a HDF5 file
        
    EFFECTS:
        The function would look for the dataset based on the dataset_path after opening up the source file.
        
    RETURN:
        The dataset found would be returned
        
    """   
    instrument = "/MODIS"
    try:
        f = h5py.File(src_filename, "r")
    except IOError:
        h5py.File.close(f)
        return None
    
    if(instrument in f):
        modis_group = f.require_group(instrument)
        valid_granule_names = []
        required_res = ["_1KM", "_250m","_500m"]
        for gname in modis_group.keys():
            res_group = f.require_group(instrument + "/" + gname)
            if(set(required_res).issubset(set(res_group.keys()))):
                valid_granule_names.append(gname)
        print("Num granules found: " + str(len(modis_group.keys())))
        print("Number of valid granules: " + str(len(valid_granule_names)))
        stitched_data = None
        ds_name = "EV_500_Aggr1km_RefSB"
        for gname in valid_granule_names:
            dname_path = instrument + "/" + gname + "/" + "_1KM" + "/" + "Data_Fields" + "/" + ds_name
            ds = f[dname_path][()]
            if(stitched_data is None):
                stitched_data = ds[0]
            else:
                stitched_data = np.concatenate([stitched_data, ds[0]], axis = 0)
        h5py.File.close(f)
        return stitched_data
    else:
        h5py.File.close(f)
        return None
        
    

    
#---------------------------------------------------------------------------------------------------
def main() :
#---------------------------------------------------------------------------------------------------
#---------------------------------------------------------------------------------------------------
   
#---------------------------------------------------------------------------------------------------
    #Default first test -- on blue radiance    
    if len(sys.argv) < 3 :
      print("usage: python image_sim.py input_files_path.txt output_files_path.txt")
      sys.exit(1)  
    
    #Open input and output paths
    input_paths = open(sys.argv[1], 'r')
    output_paths = open(sys.argv[2], 'r')
    
    #The 2 files should have equal number paths
    i_num = len(input_paths.readlines())
    o_num = len(output_paths.readlines())
    if(i_num != o_num):
        print("The number of input (" + str(i_num) + ") and output paths (" + str(o_num) + ") should be equal, something is wrong.")
        sys.exit(1)
    input_paths.seek(0)
    output_paths.seek(0)
    
    #Divide the inputs for different process 
    remainder = i_num % size
    normal_n = (i_num - remainder) / size
    start = None
    end = None
    if(rank != size - 1):
        start = rank * normal_n
        end = start + normal_n
    else:
        start = rank * normal_n
        end = start+ remainder
    
    input_paths = input_paths.readlines()[start:end]
    
    #variables to store number of good points
    good_points_list = []
    total_good_points = np.array(i_num)
    bad_orbits = []
    total_bad_orbits = np.array(i_num)
    
    #Loop through the input paths, extract the orbit number and perform checkings
    misr_regexp = re.compile(r'.*MISR.*AN.*')
    for ipath in input_paths:
        input_orbit = ipath.split('/')[-1].replace('input', '').replace('.txt\n', '')
        output_file = None
        for opath in output_paths:
            if(input_orbit in opath):
                output_file = opath.replace('\n', '')
                break
        if(output_file is None):
            print("Orbit " + str(input_orbit) + " does not have corresponding output file. Please check your files.")
            sys.exit(1)
        print(output_file)
        #Search for the MISR file of AN camera
        input_file = open(ipath.replace('\n', ''), 'r')
        misr_file_path = None
        for f in input_file:
            if(misr_regexp.search(f)):
                #MISR on MODIS matching begins here, parallel processing should be added
                #Processing MISR, select one block and save as image for comparison
                misr_file_path = f
                misr_dataset = misr_hdf4_read_dataset(misr_file_path.replace('\n', ''), 'Blue Radiance/RDQI')
                #Get block with greatest contrast
                temp_misr_name = misr_file_path.split('/')[-1].replace('.hdf\n', '') + '.png'
                misr_img_unit_convert(misr_dataset, 65504, temp_misr_name)
                #Processing MODIS, stitch all the blocks together and save as image for comparison
                stitched_modis = modis_hdf5_read(output_file)
                if(stitched_modis is None):
                    bad_orbits.append(input_orbit)
                    break
                temp_modis_name = output_file.split('/')[-1].replace('.h5\n','') + '.png'
                bf_modis_img_convert(stitched_modis, -999.0, temp_modis_name)
                #Perform matching between MISR block and MODIS stitched data
                num_good = bf_misr_modis_match(temp_misr_name, temp_modis_name)
                good_points_list.append(num_good)
                if(num_good < MIN_MATCH_COUNT):
                    bad_orbits.append(input_orbit)
                
                #Remove temporary image files
                try:
                    os.remove(temp_misr_name)
                    os.remove(temp_modis_name)
                except OSError:
                    print("File removal error")
                    sys.exit(1)
                break
                
        #Reset file pointer for next search
        output_paths.seek(0)
        input_file.close()
    print("done finding good points")
    good_points_list = np.array(good_points_list)
    bad_orbits = np.array(bad_orbits)
    
    #Sum up the results in the root process --- not tested
    comm.Reduce(good_points_list,total_good_points, MPI.SUM, root = 0)
    comm.Reduce(bad_orbits, total_bad_orbits, MPI.SUM, root = 0)
    
    if(rank == 0):
        #Output the average number of good matches and bad orbits (bad orbits may mean orbits who don't have the data or
        #orbits who have less number of good matches than the minimum)
        average_good_points = sum(total_good_points)/len(total_good_points)
        print("Average number of good points: " + str(average_good_points))
        for b in total_bad_orbits:
            print("Bad orbit: " + str(b))
        print("Total number of bad orbit: " + str(len(total_bad_orbits)))

#---------------------------------------------------------------------------------------------------
if __name__ == '__main__':
#---------------------------------------------------------------------------------------------------
   main()
   
   
"""
one block of MISR with MODIS
real    1m26.208s
user    0m47.507s
sys     0m13.489s

"""   
   
   
#Samples for calculating Structural similarity index   
"""
    #Open src input file
    print("file: " + src_filename)
    src_f = SD(src_filename, SDC.READ)
    print(src_f.info)
    b_src_obj = src_f.select('Blue Radiance/RDQI')
    r_src_obj = src_f.select('Red Radiance/RDQI')
    g_src_obj = src_f.select('Green Radiance/RDQI')
    nir_src_obj = src_f.select('NIR Radiance/RDQI')
    blue_data = b_src_obj.get()
    green_data = g_src_obj.get()
    red_data = r_src_obj.get()
    nir_data = nir_src_obj.get()
    print("Blue data shape: " + str(blue_data.shape))
    orig_arr = convert_rad_to_gray_image(blue_data, 65504, "test_misr_blue_gray.png")
    orig_green_arr = convert_rad_to_gray_image(green_data, 65504, "test_green_data.png")
    
    #Open BF output file
    print("BF file: " + bf_filename)
    bf_f = h5py.File(bf_filename, "r")
    b_bf = bf_f["/MISR/CA/Data_Fields/Blue_Radiance"][()]
    r_bf = bf_f["/MISR/CA/Data_Fields/Red_Radiance"][()]
    g_bf = bf_f["/MISR/CA/Data_Fields/Green_Radiance"][()]
    nir_bf = bf_f["/MISR/CA/Data_Fields/NIR_Radiance"][()]
    bf_arr = bf_convert_rad_to_gray_image(b_bf, -999.0, "bf_test_misr_blue_gray.png")
    gbf_arr = bf_convert_rad_to_gray_image(g_bf, -999.0, "bf_test_misr_green_gray.png")
    
    #Open BF output file
    print("BF file: " + bf_filename2)
    bf_f2 = h5py.File(bf_filename2, "r")
    b_bf2 = bf_f2["/MISR/CA/Data_Fields/Blue_Radiance"][()]
    r_bf2 = bf_f2["/MISR/CA/Data_Fields/Red_Radiance"][()]
    g_bf2 = bf_f2["/MISR/CA/Data_Fields/Green_Radiance"][()]
    nir_bf2 = bf_f2["/MISR/CA/Data_Fields/NIR_Radiance"][()]
    bf_arr2 = bf_convert_rad_to_gray_image(b_bf2, -999.0, "bf_test_misr_blue_gray.png")
    gbf_arr2 = bf_convert_rad_to_gray_image(g_bf2, -999.0, "bf_test_misr_green_gray.png")
    
    ssim_o_itself = ssim(orig_arr, orig_arr, data_range = orig_arr.max()-orig_arr.min())
    ssim_o_gb = ssim(orig_arr, orig_green_arr, data_range = orig_green_arr.max()-orig_green_arr.min())
    ssim_bf_itself = ssim(bf_arr, bf_arr, data_range=bf_arr.max() - bf_arr.min())
    ssim_obf = ssim(orig_arr, bf_arr, data_range = bf_arr.max()-bf_arr.min())
    ssim_comp_diff_rad = ssim(orig_arr, gbf_arr, data_range = gbf_arr.max()-gbf_arr.min())
    ssim_diff_orbits_same_rad = ssim(orig_arr, bf_arr2, data_range=bf_arr2.max()-bf_arr2.min())
    ssim_diff_orbits_diff_rad = ssim(orig_arr, gbf_arr2, data_range=gbf_arr2.max() - gbf_arr2.min())
    
    print("orig itself green v blue: " + str(ssim_o_gb))
    print("orig itself ssim: " + str(ssim_o_itself))
    print("bf ssim itself: " + str(ssim_bf_itself))
    print("orig bf againt: " + str(ssim_obf))
    print("orig-blue bf-green against: " + str(ssim_comp_diff_rad))
    print("same rad diff orb: " + str(ssim_diff_orbits_same_rad))
    print("diff orb diff rad: " + str(ssim_diff_orbits_diff_rad))
"""