import time
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains


class BasePage(object):
    """Base class to initialize the base page that will be called from all
    pages"""

    def __init__(self, driver):
        self.driver = driver


class MainPage(BasePage):
    """coches.net page action methods"""

    def __init__(self, driver):
        super().__init__(driver)

        """Accept cookies"""
        time.sleep(1)
        self.driver.find_element(By.XPATH, '//button[@data-testid="TcfAccept"]').click()

    def get_announcements(self):
        """Reads all announcements in the current page and moves to the last element"""
        time.sleep(1)
        announcements = self.driver.find_elements(By.CSS_SELECTOR, ".mt-CardBasic")
        actions = ActionChains(self.driver)
        actions.move_to_element(announcements[-1]).perform()
        return announcements
