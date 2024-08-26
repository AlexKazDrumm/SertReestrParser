import aiohttp
import asyncio
from bs4 import BeautifulSoup
from datetime import datetime

async def fetch(session, url):
    async with session.get(url) as response:
        return await response.text()

def is_valid_date(date_text):
    try:
        expiration_date = datetime.strptime(date_text, "%d.%m.%Y")
        current_date = datetime.now()
        return expiration_date >= current_date
    except ValueError:
        return False

async def parse_document(session, document_id):
    url = f"https://sert-reestr.net/ss_product_{document_id:06d}"
    try:
        response_text = await fetch(session, url)
        soup = BeautifulSoup(response_text, 'html.parser')

        # Поиск даты окончания
        dt_tag = soup.find("dt", string="Дата окончания:")
        expiration_date = None
        if dt_tag:
            dd_tag = dt_tag.find_next("dd")
            if dd_tag:
                expiration_date = dd_tag.text.strip()

        # Поиск заявителя
        applicant_tag = soup.find("dt", string="Заявитель:")
        applicant = None
        if applicant_tag:
            applicant_dd_tag = applicant_tag.find_next("dd")
            if applicant_dd_tag:
                applicant = applicant_dd_tag.text.strip()

        if expiration_date and is_valid_date(expiration_date):
            return url, expiration_date, applicant

    except Exception as e:
        return None, None, f"Error parsing document {document_id}: {e}"

    return None, None, None

async def main():
    total_documents = 60000  # Количество проверяемых страниц
    valid_links = []
    valid_count = 0
    async with aiohttp.ClientSession() as session:
        tasks = []
        with open('valid_documents.txt', 'w', encoding='utf-8') as file, open('errors.txt', 'w', encoding='utf-8') as error_file:
            for document_id in range(total_documents):
                tasks.append(parse_document(session, document_id))
                if len(tasks) >= 100:  # Batch size of 100
                    results = await asyncio.gather(*tasks)
                    for result, expiration_date, applicant in results:
                        if result:
                            valid_count += 1
                            valid_links.append((result, expiration_date, applicant))
                            progress = (document_id / total_documents) * 100
                            print(
                                f"{valid_count}. Valid document found: {result} | Expiration Date: {expiration_date} | Applicant: {applicant} | Progress: {progress:.2f}% ({document_id}/{total_documents})")
                            file.write(
                                f"{valid_count}. {result} | Expiration Date: {expiration_date} | Applicant: {applicant}\n")
                            file.flush()  # Ensure data is written to the file
                        elif applicant:
                            print(applicant)
                            error_file.write(f"{applicant}\n")
                            error_file.flush()
                    tasks = []

                # Print progress
                if document_id % 100 == 0:
                    progress = (document_id / total_documents) * 100
                    print(f"Progress: {progress:.2f}% ({document_id}/{total_documents})", end='\r')

            # Handle remaining tasks
            if tasks:
                results = await asyncio.gather(*tasks)
                for result, expiration_date, applicant in results:
                    if result:
                        valid_count += 1
                        valid_links.append((result, expiration_date, applicant))
                        progress = (total_documents / total_documents) * 100
                        print(
                            f"{valid_count}. Valid document found: {result} | Expiration Date: {expiration_date} | Applicant: {applicant} | Progress: {progress:.2f}% ({total_documents}/{total_documents})")
                        file.write(
                            f"{valid_count}. {result} | Expiration Date: {expiration_date} | Applicant: {applicant}\n")
                        file.flush()  # Ensure data is written to the file
                    elif applicant:
                        print(applicant)
                        error_file.write(f"{applicant}\n")
                        error_file.flush()

    print("\nValid documents:")
    for index, (link, expiration_date, applicant) in enumerate(valid_links, start=1):
        print(f"{index}. {link} | Expiration Date: {expiration_date} | Applicant: {applicant}")

if __name__ == "__main__":
    asyncio.run(main())