import time
import os
import logging
from urllib.parse import urlparse
from urllib.request import urlretrieve
from queue import Queue
from threading import Thread
import multiprocessing
import asyncio
import aiofiles
import aiohttp

import PIL
from PIL import Image

logging.basicConfig(filename='logfile.log', level=logging.DEBUG)


class ThumbnailMakerService(object):
    def __init__(self, home_dir='.'):
        self.home_dir = home_dir
        self.input_dir = self.home_dir + os.path.sep + 'incoming'
        self.output_dir = self.home_dir + os.path.sep + 'outgoing'
        self.img_queue = multiprocessing.JoinableQueue()
        # need the size of original files and of resized file
        self.dl_size = 0
        self.resized_size = multiprocessing.Value("i", 0)

    # new
    async def download_image_coro(self, session, url):
        img_filename = urlparse(url).path.split('/')[-1]
        img_filepath = self.input_dir + os.path.sep + img_filename

        async with session.get(url) as response:
            # async write to file
            async with aiofiles.open(img_filepath, 'wb') as f:
                content = await response.content.read()
                await f.write(content)

        self.dl_size += os.path.getsize(img_filepath)
        self.img_queue.put(img_filename)
    # new

    async def download_images_coro(self, img_url_list):
        async with aiohttp.ClientSession() as session:
            for url in img_url_list:
                await self.download_image_coro(session, url)

    def download_images(self, img_url_list):
        if not img_url_list:
            return
        os.makedirs(self.input_dir, exist_ok=True)

        logging.info("beginning image downloads")

        start = time.perf_counter()
        # new
        loop = asyncio.get_event_loop()  # register a loop
        try:
            loop.run_until_complete(self.download_images_coro(
                img_url_list))  # a coro function
        finally:
            loop.close()
        end = time.perf_counter()

        logging.info("downloaded {} images in {} seconds".format(
            len(img_url_list), end - start))

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
                    # new
                    out_filepath = self.output_dir + os.path.sep + new_filename
                    img.save(out_filepath)

                    with self.resized_size.get_lock():
                        self.resized_size.value += os.path.getsize(
                            out_filepath)

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

        num_processes = multiprocessing.cpu_count()
        for _ in range(num_processes):
            p = multiprocessing.Process(target=self.perform_resizing)
            p.start()

        # new
        # need to put it afater the process above starts
        # so immediately when an img download it can be picked up by resize

        self.download_images(img_url_list)

        for _ in range(num_processes):
            self.img_queue.put(None)

        end = time.perf_counter()
        logging.info("END make_thumbnails in {} seconds".format(end - start))
        logging.info("Initial size of downloads: [{}] Final size of images: [{}] ".format(
            self.dl_size, self.resized_size))
