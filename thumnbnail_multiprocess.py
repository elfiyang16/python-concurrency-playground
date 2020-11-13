# thumbnail_maker.py
import time
import os
import logging
from urllib.parse import urlparse
from urllib.request import urlretrieve
from queue import Queue
from threading import Thread
import multiprocessing

import PIL
from PIL import Image

logging.basicConfig(filename='logfile.log', level=logging.DEBUG)


class ThumbnailMakerService(object):
    def __init__(self, home_dir='.'):
        self.home_dir = home_dir
        self.input_dir = self.home_dir + os.path.sep + 'incoming'
        self.output_dir = self.home_dir + os.path.sep + 'outgoing'
        # self.img_queue = Queue()
        # transform the queue to a list so can be passed in as iter to Pool
        self.img_list = []
        # remove the queue as all attri of the target function
        # has to be pickable in resize_image
        # but queue is not
        # self.dl_queue = Queue()

    def download_image(self, dl_queue):
        while not dl_queue.empty():
            try:
                url = dl_queue.get(block=False)
                img_filename = urlparse(url).path.split('/')[-1]
                urlretrieve(url, self.input_dir + os.path.sep + img_filename)
                # self.img_queue.put(img_filename)
                # append to the list
                self.img_list.append(img_filename)

                dl_queue.task_done()
            except Queue.Empty:
                logging.info("Queue empty")

    def download_images(self, img_url_list):
        if not img_url_list:
            return
        os.makedirs(self.input_dir, exist_ok=True)

        logging.info("beginning image downloads")

        start = time.perf_counter()
        for url in img_url_list:
            img_filename = urlparse(url).path.split('/')[-1]
            urlretrieve(url, self.input_dir + os.path.sep + img_filename)
            self.img_queue.put(img_filename)

        end = time.perf_counter()
        self.img_queue.put(None)

        logging.info("downloaded {} images in {} seconds".format(
            len(img_url_list), end - start))

    # NEW
    def resize_image(self, filename):
        target_sizes = [32, 64, 200]

        logging.info("resizing image {}".format(filename))
        orig_img = Image.open(self.input_dir + os.path.sep + filename)
        for basewidth in target_sizes:
            img = orig_img
            wpercent = (basewidth / float(img.size[0]))
            hsize = int((float(img.size[1]) * float(wpercent)))
            img = img.resize((basewidth, hsize), PIL.Image.LANCZOS)
            new_filename = os.path.splitext(filename)[0] + \
                '_' + str(basewidth) + os.path.splitext(filename)[1]
            img.save(self.output_dir + os.path.sep + new_filename)

        os.remove(self.input_dir + os.path.sep + filename)
        logging.info("done resizing image {}".format(filename))

    def perform_resizing(self):
        os.makedirs(self.output_dir, exist_ok=True)
        logging.info("beginning image resizing")

        target_sizes = [32, 64, 200]
        num_images = len(os.listdir(self.input_dir))

        start = time.perf_counter()
        while True:
            filename = self.img_queue.get()
            if filename:
                logging.info("resizing image {}".format(filename))
                orig_img = Image.open(self.input_dir + os.path.sep + filename)
                for basewidth in target_sizes:
                    img = orig_img
                    wpercent = (basewidth / float(img.size[0]))
                    hsize = int((float(img.size[1]) * float(wpercent)))
                    img = img.resize((basewidth, hsize), PIL.Image.LANCZOS)
                    new_filename = os.path.splitext(filename)[0] + \
                        '_' + str(basewidth) + os.path.splitext(filename)[1]
                    img.save(self.output_dir + os.path.sep + new_filename)

                os.remove(self.input_dir + os.path.sep + filename)
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
        # create dl_queue as local var and pass in
        dl_queue = Queue()

        for _ in img_url_list:
            dl_queue.put(_)
        num_dl_threads = 4
        for _ in range(num_dl_threads):
            t = Thread(target=self.download_image, args=(dl_queue,))
            t.start()
        dl_queue.join()

        # self.download_images(img_url_list)
        # self.perform_resizing()

        # NEW:
        start_resize = time.perf_counter()
        pool = multiprocessing.Pool()
        pool.map(self.resize_image, self.img_list)
        end_resize = time.perf_counter()

        end = time.perf_counter()
        pool.close()
        pool.join()
        logging.info("created {} thumbnails in {} seconds".format(
            len(self.img_list), end_resize-start_resize))
        logging.info("END make_thumbnails in {} seconds".format(end - start))
