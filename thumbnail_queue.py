# thumbnail_maker.py
import time
import os
import logging
from urllib.parse import urlparse
from urllib.request import urlretrieve
from queue import Queue
from threading import Thread

import PIL
from PIL import Image

logging.basicConfig(filename='logfile.log', level=logging.DEBUG)


class ThumbnailMakerService(object):
    def __init__(self, home_dir='.'):
        self.home_dir = home_dir
        self.input_dir = self.home_dir + os.path.sep + 'incoming'
        self.output_dir = self.home_dir + os.path.sep + 'outgoing'
        self.img_queue = Queue()
        self.dl_queue = Queue()

    # this is new:
    def download_image(self):
        while not self.dl_queue.empty():
            try:  # passing non-block flag will trigger exception if que is empty
                url = self.dl_queue.get(block=False)
                # download each image and save to the input dir
                img_filename = urlparse(url).path.split('/')[-1]
                urlretrieve(url, self.input_dir + os.path.sep + img_filename)
                # put the downloaded file into the queue
                self.img_queue.put(img_filename)

                self.dl_queue.task_done()
            except Queue.Empty:
                """
                reason to have another guard clause here checking if empty:
                thread A checks if the queue is empty when only one item in the queue
                thread A checks non-empty
                while thread B finishes check and fetch the last item first
                thread A trying to fetch an empty queue
                """
                logging.info("Queue empty")

    def download_images(self, img_url_list):
        # validate inputs
        if not img_url_list:  # if null
            return
        os.makedirs(self.input_dir, exist_ok=True)

        logging.info("beginning image downloads")

        start = time.perf_counter()
        for url in img_url_list:
          # download each image and save to the input dir
            img_filename = urlparse(url).path.split('/')[-1]
            urlretrieve(url, self.input_dir + os.path.sep + img_filename)
            # put the downloaded file into the queue
            self.img_queue.put(img_filename)

        end = time.perf_counter()

        # poison pill: for producer to notify consumer there's nothing inqueue
        self.img_queue.put(None)

        logging.info("downloaded {} images in {} seconds".format(
            len(img_url_list), end - start))

    def perform_resizing(self):
        # no longer need below as it's reading from queue
        # if not os.listdir(self.input_dir):
        #     return
        os.makedirs(self.output_dir, exist_ok=True)

        logging.info("beginning image resizing")

        target_sizes = [32, 64, 200]
        num_images = len(os.listdir(self.input_dir))

        start = time.perf_counter()
        # consume from the queue
        while True:
            filename = self.img_queue.get()
            # if filename != falsy --> still item in queue
            if filename:
                logging.info("resizing image {}".format(filename))
            # for filename in os.listdir(self.input_dir):
                orig_img = Image.open(self.input_dir + os.path.sep + filename)
                for basewidth in target_sizes:
                    img = orig_img
                    # calculate target height of the resized image to maintain the aspect ratio
                    wpercent = (basewidth / float(img.size[0]))
                    hsize = int((float(img.size[1]) * float(wpercent)))
                    # perform resizing
                    img = img.resize((basewidth, hsize), PIL.Image.LANCZOS)

                    # save the resized image to the output dir with a modified file name
                    new_filename = os.path.splitext(filename)[0] + \
                        '_' + str(basewidth) + os.path.splitext(filename)[1]
                    img.save(self.output_dir + os.path.sep + new_filename)

                os.remove(self.input_dir + os.path.sep + filename)
                # mark as done after resizing
                logging.info("done resizing image {}".format(filename))
                self.img_queue.task_done()
            else:
                self.img_queue.task_done()
                break

        end = time.perf_counter()

        logging.info("created {} thumbnails in {} seconds".format(
            num_images, end - start))

    def make_thumbnails(self, img_url_list):
        logging.info("START make_thumbnails")

        start = time.perf_counter()

        # load img url into dl_Queue
        for _ in img_url_list:
            self.dl_queue.put(_)

        num_dl_threads = 4
        for _ in range(num_dl_threads):
            t = Thread(target=self.download_image)
            t.start()

        """
        comment out the single producer below using download_images:
        note that in above multi-producer (4) above 
        there's no call on .join()
        nor is there a poison pill in the download_image
        this is to prevent if one producer (A) finishes and sends the pill
        the whole consumer (resizing method) will block the tunnel 
        causing other 3 producers stop working
        """

        # t1 = Thread(target=self.download_images, args=([img_url_list]))
        t2 = Thread(target=self.perform_resizing)
        # t1.start()
        t2.start()
        # t1.join()

        # block main thread until all download completes: like promise.all()
        self.dl_queue.join()
        # and then insert the pill in img_queue - a holder for all downloaded imgs
        self.img_queue.put(None)
        t2.join()

        # self.download_images(img_url_list)
        # self.perform_resizing()

        end = time.perf_counter()
        logging.info("END make_thumbnails in {} seconds".format(end - start))
