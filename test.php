<?
define ("PARTITION", 500, true);
define ("CATALOG_IBLOCK_ID", 5, true);
define ("OFFERS_IBLOCK_ID", 6, true);
define ("VENDORS_IBLOCK_ID", 7, true);
define ("COLORS_HIGHILOADBLOCK_ID", 3, true);

function ImportMainCatalog()
{
    /**
     * Лог файл
     */

    $logFile = $_SERVER["DOCUMENT_ROOT"] . '/sync/log/' .date("y-m-d")."-import-import-main-catalog-log_".time().".txt";
    $currentLog = file_get_contents($logFile);

    $addToImportLog = function ($text) use (&$currentLog) {
        $currentLog .= $text . "\n";
    };

    $addToImportLog("\n---------- started log ---------\n");

    /**
     * скачиваем файл
     */

    $fileUrl = 'http://stripmag.ru/datafeed/bitrix.csv';
    $saveTo = $_SERVER["DOCUMENT_ROOT"] . "/sync/upload_csv/bitrix.csv";

    $bytes = curl_download($fileUrl, $saveTo);

    if(intval($bytes) > 0){
        $handle = fopen($saveTo, "r");

        if(($buffer = fgets($handle)) == false) {
            $addToImportLog("Ошибка. Файл пустой.\n");
        }
        else {
            $addToImportLog("OK. Файл успешно скачан.\n");

            $nextExec = ConvertTimeStamp(time()+10, "FULL");
            CAgent::AddAgent(
                "ImportMainCatalogStep(0, " . PARTITION .");", // имя функции
                "iblock",                   // идентификатор модуля
                "N",                         // агент не критичен к кол-ву запусков
                1,                          // интервал запуска - 1 сутки
                $nextExec,                          // дата первой проверки на запуск
                "Y",                         // агент активен
                $nextExec,                          // дата первого запуска
                30);
        }
    }

    $addToImportLog("\n---------- ended   log ---------");
    file_put_contents($logFile, $currentLog);

    return "ImportMainCatalog();";
}
function ImportMainCatalogStep($startLine, $partitional)
{
    CModule::IncludeModule("iblock");

    $csvFile = $_SERVER["DOCUMENT_ROOT"] . "/sync/upload_csv/bitrix.csv";

    /**
     * Лог файл
     */

    $logFile = $_SERVER["DOCUMENT_ROOT"] . '/sync/log/' .date("y-m-d")."-import-import-main-catalog-step-" . $startLine ."-log_".time().".txt";
    $currentLog = file_get_contents($logFile);

    $addToImportLog = function ($text) use (&$currentLog) {
        $currentLog .= $text . "\n";
    };

    $addToImportLog("\n---------- started log ---------\n");

    $handle = fopen($csvFile, "r");

    $paramsTranslit = Array(
        "max_len" => "100", // обрезает символьный код до 100 символов
        "change_case" => "L", // буквы преобразуются к нижнему регистру
        "replace_space" => "_", // меняем пробелы на нижнее подчеркивание
        "replace_other" => "_", // меняем левые символы на нижнее подчеркивание
        "delete_repeat_replace" => "true", // удаляем повторяющиеся нижние подчеркивания
        "use_google" => "false", // отключаем использование google
    );
    $el = new CIBlockElement;

    $iteration = 0;
    $iteration_inner = 0;
    $tmp_startLine = $startLine;

    while(($buffer = fgets($handle)) !== false) {
        $row = explode(";", $buffer);

        if($startLine !== 0) {

            if (($tmp_startLine == 0) || ($iteration == $tmp_startLine)) {

                $prod_id = $row[0];
                $name_element = str_replace("\"", "", $row[3]);
                $detail_text = str_replace("\"", "", $row[4]);

                $vendor_id = $row[1];
                $vendor_code = str_replace("\"", "", $row[2]);
                $batteries = str_replace("\"", "", $row[15]);
                $pack = str_replace("\"", "", $row[16]);
                $material = str_replace("\"", "", $row[17]);
                $length = $row[18];
                $diameter = $row[19];
                $collection = str_replace("\"", "", $row[20]);

                $section_level_01 = str_replace("\"", "", $row[21]);
                $section_level_02 = str_replace("\"", "", $row[22]);
                $section_level_03 = str_replace("\"", "", $row[23]);

                $bestseller = $row[24];
                $new = $row[25];
                $function = str_replace("\"", "", $row[26]);
                $add_function = str_replace("\"", "", $row[27]);
                $vibration = $row[28];
                $volume = $row[29];
                $model_year = $row[30];
                $price = $row[31];
                $image_status = $row[32];


                /**
                 * Разделы
                 */

                $PARENT_ID = false;
                if (strlen($section_level_01) > 0) {

                    $arSections = makeArraySection($section_level_01, $section_level_02, $section_level_03);

                    foreach ($arSections as $section) {
                        $rsSection = CIBlockSection::GetList(
                            array(),
                            array(
                                "SECTION_ID" => $PARENT_ID,
                                "NAME" => $section,
                                "IBLOCK_ID" => CATALOG_IBLOCK_ID
                            )
                        );

                        if ($arSection = $rsSection->GetNext()) {
                            $PARENT_ID = $arSection["ID"];
                        } else {
                            $bs = new CIBlockSection;
                            $arFields = Array(
                                "ACTIVE" => "Y",
                                "IBLOCK_SECTION_ID" => $PARENT_ID,
                                "IBLOCK_ID" => CATALOG_IBLOCK_ID,
                                "NAME" => $section
                            );

                            $PARENT_ID = $bs->Add($arFields);

                            if (!$PARENT_ID) {
                                $addToImportLog($bs->LAST_ERROR);
                            }
                        }
                    }
                }

                $exId = $prod_id;

                /**
                 * Элементы
                 */
                /**
                 *  Свойства
                 */

                $arProperties[$exId]["PROD_ID"] = $prod_id;
                $arProperties[$exId]["CML2_VENDOR"] = $vendor_id;
                $arProperties[$exId]["ARTICULE"] = $vendor_code;

                $arProperties[$exId]["BATTERIES"] = $batteries;
                $arProperties[$exId]["PACK"] = $pack;
                $arProperties[$exId]["MATERIAL"] = $material;
                $arProperties[$exId]["LENGTH_CM"] = $length;
                $arProperties[$exId]["DIAMETER"] = $diameter;
                $arProperties[$exId]["COLLECTION"] = $collection;

                //BESTSELLER
                if (intval($bestseller))
                    $bestseller = Array("VALUE" => "17"); // 17 - ID значения свойства
                else
                    $bestseller = Array("VALUE" => "18");

                //NEW
                if (intval($new))
                    $new = Array("VALUE" => "19");
                else
                    $new = Array("VALUE" => "20");

                //IMG_STATUS
                if ($image_status == "1")
                    $image_status = Array("VALUE" => "21");
                elseif ($image_status == "2")
                    $image_status = Array("VALUE" => "22");
                elseif ($image_status == "3")
                    $image_status = Array("VALUE" => "23");

                $arProperties[$exId]["BESTSELLER"] = $bestseller;
                $arProperties[$exId]["NEW"] = $new;
                $arProperties[$exId]["FUNCTION"] = $function;
                $arProperties[$exId]["ADD_FUNCTION"] = $add_function;
                $arProperties[$exId]["VIBRATION"] = $vibration;
                $arProperties[$exId]["VOLUME"] = $volume;
                $arProperties[$exId]["MODEL_YEAR"] = $model_year;
                $arProperties[$exId]["IMG_STATUS"] = $image_status;


                $rsElem = CIBlockElement::Getlist(
                    array(),
                    array(
                        "EXTERNAL_ID" => $prod_id,
                        "IBLOCK_ID" => CATALOG_IBLOCK_ID,
                        "SECTION_ID" => $PARENT_ID
                    )
                );

                //если существует элементо, то обновить его
                if ($elem = $rsElem->GetNext()) {

                    $arLoadProductArray = Array(
                        "EXTERNAL_ID" => $prod_id,
                        "IBLOCK_SECTION_ID" => $PARENT_ID,
                        "IBLOCK_ID" => CATALOG_IBLOCK_ID,
                        "CODE" => CUtil::translit($name_element . "_" . $section_level_02 . "_" . $section_level_03, "ru", $paramsTranslit),
                        "NAME" => $name_element,
                        "ACTIVE" => "Y",
                        "DETAIL_TEXT" => $detail_text,
                    );

                    $el->Update($elem["ID"], $arLoadProductArray);

                    CIBlockElement::SetPropertyValuesEx($elem["ID"], false, $arProperties[$exId]);

                    /***
                     * Цена товара
                     */

                    //добавляет параметры товара
                    $arCCatalogProductFields = array(
                        "ID" => $elem["ID"],
                        "VAT_ID" => 1, //тип НДС
                        "VAT_INCLUDED" => "Y" //НДС входит в стоимость
                    );
                    if (CCatalogProduct::Add($arCCatalogProductFields))
                        $addToImportLog("Добавлены параметры (CCatalogProduct::Add) товара к элементу каталога " . $elem["ID"] . "\n");
                    else
                        $addToImportLog("Ошибка добавления параметров цены\n");

                    $PRICE_TYPE_ID = 1;

                    $arPriceFields = Array(
                        "PRODUCT_ID" => $elem["ID"],
                        "CATALOG_GROUP_ID" => $PRICE_TYPE_ID,
                        "PRICE" => $price,
                        "CURRENCY" => "RUB",
                        "QUANTITY_FROM" => false,
                        "QUANTITY_TO" => false
                    );

                    $res = CPrice::GetList(
                        array(),
                        array(
                            "PRODUCT_ID" => $elem["ID"],
                            "CATALOG_GROUP_ID" => $PRICE_TYPE_ID
                        )
                    );

                    if ($arr = $res->Fetch()) {
                        CPrice::Update($arr["ID"], $arPriceFields);
                        $addToImportLog("Базова цена элемента обновлена\n");
                    } else {
                        CPrice::Add($arPriceFields);
                        $addToImportLog("Базова цена элемента добавлена\n");
                    }

                    $addToImportLog("Изменен ID: " . $elem["ID"] . "\t product_id: " . $prod_id . "\n");

                } else {

                    $arLoadProductArray = Array(
                        "EXTERNAL_ID" => $prod_id,
                        "IBLOCK_SECTION_ID" => $PARENT_ID,
                        "IBLOCK_ID" => CATALOG_IBLOCK_ID,
                        "CODE" => CUtil::translit($name_element . "_" . $section_level_02 . "_" . $section_level_03, "ru", $paramsTranslit),
                        "PROPERTY_VALUES" => $arProperties[$exId],
                        "NAME" => $name_element,
                        "ACTIVE" => "Y",
                        "DETAIL_TEXT" => $detail_text,
                    );

                    if ($PRODUCT_ID = $el->Add($arLoadProductArray)) {

                        /***
                         * Цена товара
                         */

                        //добавляет параметры товара
                        $arCCatalogProductFields = array(
                            "ID" => $PRODUCT_ID,
                            "VAT_ID" => 1, //тип НДС
                            "VAT_INCLUDED" => "Y" //НДС входит в стоимость
                        );
                        if (CCatalogProduct::Add($arCCatalogProductFields))
                            $addToImportLog("Добавлены параметры товара к элементу каталога " . $PRODUCT_ID . "\n");
                        else
                            $addToImportLog("Ошибка добавления параметров цены\n");

                        $PRICE_TYPE_ID = 1;

                        $arPriceFields = Array(
                            "PRODUCT_ID" => $PRODUCT_ID,
                            "CATALOG_GROUP_ID" => $PRICE_TYPE_ID,
                            "PRICE" => $price,
                            "CURRENCY" => "RUB",
                            "QUANTITY_FROM" => false,
                            "QUANTITY_TO" => false
                        );

                        $res = CPrice::GetList(
                            array(),
                            array(
                                "PRODUCT_ID" => $PRODUCT_ID,
                                "CATALOG_GROUP_ID" => $PRICE_TYPE_ID
                            )
                        );

                        if ($arr = $res->Fetch()) {
                            CPrice::Update($arr["ID"], $arPriceFields);
                            $addToImportLog("Базова цена элемента обновлена\n");
                        } else {
                            CPrice::Add($arPriceFields);
                            $addToImportLog("Базова цена элемента добавлена\n");
                        }


                        $addToImportLog("Новый ID: " . $PRODUCT_ID . "\t product_id: " . $prod_id . "\n");
                    }

                }

                $startLine++;
                $iteration_inner++;

                if ($iteration_inner == PARTITION) {

                    $partitional = $startLine + PARTITION;

                    $addToImportLog("\n---------- ended   log ---------");
                    file_put_contents($logFile, $currentLog);

                    return "ImportMainCatalogStep(" . $startLine . ", " . $partitional . ");";

                }
            } elseif ($tmp_startLine !== 0) {
                $iteration++;
            }
        }
        elseif($startLine == 0) {
            $startLine++;
        }

    }

    $addToImportLog("\n---------- ended   log ---------");
    file_put_contents($logFile, $currentLog);

}

function ImportPicturesCatalog(){
    /**
     * Лог файл
     */

    $logFile = $_SERVER["DOCUMENT_ROOT"] . '/sync/log/' .date("y-m-d")."-import-import-pictures-catalog-log_".time().".txt";
    $currentLog = file_get_contents($logFile);

    $addToImportLog = function ($text) use (&$currentLog) {
        $currentLog .= $text . "\n";
    };

    $addToImportLog("\n---------- started log ---------\n");

    /**
     * скачиваем файл
     */

    $fileUrl = 'http://stripmag.ru/datafeed/bitrix.csv';
    $saveTo = $_SERVER["DOCUMENT_ROOT"] . "/sync/upload_csv/bitrix_pictures.csv";

    $bytes = curl_download($fileUrl, $saveTo);

    if(intval($bytes) > 0){
        $handle = fopen($saveTo, "r");

        if(($buffer = fgets($handle)) == false) {
            $addToImportLog("Ошибка. Файл пустой.\n");
        }
        else {
            $addToImportLog("OK. Файл успешно скачан.\n");

            $nextExec = ConvertTimeStamp(time()+10, "FULL");
            CAgent::AddAgent(
                "ImportPicturesCatalogStep(0, " . PARTITION .");", // имя функции
                "",                   // идентификатор модуля
                "N",                         // агент не критичен к кол-ву запусков
                1,                          // интервал запуска - 1 сутки
                $nextExec,                          // дата первой проверки на запуск
                "Y",                         // агент активен
                $nextExec,                          // дата первого запуска
                30);
        }
    }

    $addToImportLog("\n---------- ended   log ---------");
    file_put_contents($logFile, $currentLog);

    return "ImportPicturesCatalog();";
}
function ImportPicturesCatalogStep($startLine, $partitional) {
    CModule::IncludeModule("iblock");

    $csvFile = $_SERVER["DOCUMENT_ROOT"] . "/sync/upload_csv/bitrix.csv";

    /**
     * Лог файл
     */

    $logFile = $_SERVER["DOCUMENT_ROOT"] . '/sync/log/' .date("y-m-d")."-import-import-pictures-catalog-step-" . $startLine ."-log_".time().".txt";
    $currentLog = file_get_contents($logFile);

    $addToImportLog = function ($text) use (&$currentLog) {
        $currentLog .= $text . "\n";
    };

    $addToImportLog("\n---------- started log ---------\n");

    $handle = fopen($csvFile, "r");

    $el = new CIBlockElement;
    $io = CBXVirtualIo::GetInstance();

    $iteration = 0;
    $iteration_inner = 0;
    $tmp_startLine = $startLine;

    $arDeleteImages = array();

    while(($buffer = fgets($handle)) !== false) {
        $row = explode(";", $buffer);

        if($startLine !== 0) {

            if (($tmp_startLine == 0) || ($iteration == $tmp_startLine)) {

                $prod_id = $row[0];
                $url_picture = $row[5];

                if(!empty($prod_id)) {

                    $arImage = array();
                    for($i = 0, $count = 6; $i < 9; $i++, $count++){
                        if(!empty($row[$count])) {
                            $temp_name = $io->GetLogicalName(bx_basename($row[$count]));
                            $temp_name = str_replace("-", "_", $temp_name);
                            $saveTo = $_SERVER["DOCUMENT_ROOT"] . "/sync/upload_images/" . $temp_name;
                            $bytes = curl_download($row[$count], $saveTo);

                            if(intval($bytes) > 0) {
                                $arImage["n$i"] = Array(
                                    "VALUE" => CFile::MakeFileArray($saveTo)
                                );

                                $addToImportLog("n$i) " . $arImage["n$i"]["VALUE"]["name"]);
                                $addToImportLog("n$i) " . $arImage["n$i"]["VALUE"]["size"]);
                                $addToImportLog("n$i) " . $arImage["n$i"]["VALUE"]["tmp_name"]);
                                $addToImportLog("n$i) " . $arImage["n$i"]["VALUE"]["type"] . "\n");
                            }

                            $arDeleteImages[] = $saveTo;
                        }
                    }

                    $temp_name = $io->GetLogicalName(bx_basename($url_picture));
                    $saveTo = $_SERVER["DOCUMENT_ROOT"] . "/sync/upload_images/" . $temp_name;
                    $bytes = curl_download($url_picture, $saveTo);
                    $arDeleteImages[] = $saveTo;

                    if(intval($bytes) > 0) {

                        $picture = CFile::MakeFileArray($saveTo);

                        $arLoadProductArray = Array(
                            "PREVIEW_PICTURE" => $picture,
                            "DETAIL_PICTURE" => $picture,
                        );

                        $rsElem = CIBlockElement::Getlist(
                            array(),
                            array(
                                "EXTERNAL_ID" => $prod_id,
                                "IBLOCK_ID" => CATALOG_IBLOCK_ID,
                            ),
                            false,
                            false,
                            array("ID")
                        );

                        if ($arElem = $rsElem->Fetch()) {
                            $res = $el->Update($arElem["ID"], $arLoadProductArray);
                            if ($res)
                                $addToImportLog("\nDETAIL и PREVIEW картинки обвнолены - [" . $arElem["ID"] . "]\n");
                            else
                                $addToImportLog("\n$el->LAST_ERROR - [" . $arElem["ID"] . "]\n");

                            CIBlockElement::SetPropertyValuesEx($arElem["ID"], false, array("PHOTOS" => $arImage));

                        }
                    }

                    foreach ($arDeleteImages as $image) {
                        unlink($image);
                    }

                }

                $startLine++;
                $iteration_inner++;

                if ($iteration_inner == PARTITION) {

                    $partitional = $startLine + PARTITION;

                    $addToImportLog("\n---------- ended   log ---------");
                    file_put_contents($logFile, $currentLog);

                    return "ImportPicturesCatalogStep(" . $startLine . ", " . $partitional . ");";

                }
            } elseif ($tmp_startLine !== 0) {
                $iteration++;
            }
        }
        elseif($startLine == 0){
            $startLine++;
        }

    }
    $addToImportLog("\n---------- ended   log ---------");
    file_put_contents($logFile, $currentLog);

}

function ImportOffersFullCatalog(){
    /**
     * Лог файл
     */

    $logFile = $_SERVER["DOCUMENT_ROOT"] . '/sync/log/' .date("y-m-d")."-import-import-offers-full-log_".time().".txt";
    $currentLog = file_get_contents($logFile);

    $addToImportLog = function ($text) use (&$currentLog) {
        $currentLog .= $text . "\n";
    };

    $addToImportLog("\n---------- started log ---------\n");

    /**
     * скачиваем файл
     */

    $fileUrl = 'http://stripmag.ru/datafeed/bitrix_stock.csv';
    $saveTo = $_SERVER["DOCUMENT_ROOT"] . "/sync/upload_csv/bitrix_stock.csv";

    $bytes = curl_download($fileUrl, $saveTo);

    if(intval($bytes) > 0){
        $handle = fopen($saveTo, "r");

        if(($buffer = fgets($handle)) == false) {
            $addToImportLog("Ошибка. Файл пустой.\n");
        }
        else {
            $addToImportLog("OK. Файл успешно скачан.\n");

            $nextExec = ConvertTimeStamp(time()+10, "FULL");
            CAgent::AddAgent(
                "ImportOffersFullCatalogStep(0, " . PARTITION .");", // имя функции
                "",                         // идентификатор модуля
                "N",                         // агент не критичен к кол-ву запусков
                1,                          // интервал запуска - 1 секунда
                $nextExec,                          // дата первой проверки на запуск
                "Y",                         // агент активен
                $nextExec,                          // дата первого запуска
                30);
        }
    }

    $addToImportLog("\n---------- ended   log ---------");
    file_put_contents($logFile, $currentLog);

    return "ImportOffersFullCatalog();";
}
function ImportOffersFullCatalogStep($startLine, $partitional){
    CModule::IncludeModule("iblock");
    CModule::IncludeModule("catalog");

    $csvFile = $_SERVER["DOCUMENT_ROOT"] . "/sync/upload_csv/bitrix_stock.csv";

    /**
     * Лог файл
     */

    $logFile = $_SERVER["DOCUMENT_ROOT"] . '/sync/log/' .date("y-m-d")."-import-import-offers-full-step-" . $startLine ."-log_".time().".txt";
    $currentLog = file_get_contents($logFile);

    $addToImportLog = function ($text) use (&$currentLog) {
        $currentLog .= $text . "\n";
    };

    $addToImportLog("\n---------- started log ---------\n");

    $handle = fopen($csvFile, "r");

    $iteration = 0;
    $iteration_inner = 0;
    $tmp_startLine = $startLine;

    while(($buffer = fgets($handle)) !== false) {
        $row = explode(";", $buffer);

        if($startLine !== 0) {

            if (($tmp_startLine == 0) || ($iteration == $tmp_startLine)) {

                $prod_id = $row[0];

                if(!empty($prod_id)) {

                    $addToImportLog("---------- offer ---------");

                    $sku = $row[1];
                    $barcode = $row[2];
                    $offerName = str_replace("\"", "", $row[3]);
                    $quantity = $row[4];
                    $shippingdate = str_replace("\"", "", $row[5]);
                    $weight = $row[6];
                    $color = str_replace("\"", "", $row[7]);
                    $size = $row[8];
                    $currency = $row[9];
                    $price = $row[10];
                    $basewholeprice = $row[11];
                    $quantity_storage = $row[12];
                    $super_sale = $row[13];
                    $stop_promo = $row[14];

                    if($stop_promo == 1)
                        $stop_promo = 26; // 26 - ID значения
                    else
                        $stop_promo = 27;

                    $arCatalog = CCatalog::GetByID(OFFERS_IBLOCK_ID);

                    $SKUPropertyId = $arCatalog['SKU_PROPERTY_ID']; // ID свойства в инфоблоке предложений типа "Привязка к товарам (SKU)"

                    $rsElem = CIBlockElement::Getlist(
                        array(),
                        array(
                            "EXTERNAL_ID" => $prod_id,
                            "IBLOCK_ID" => CATALOG_IBLOCK_ID,
                        )
                    );

                    if ($product = $rsElem->Fetch())
                    {
                        $arInfo = CCatalogSKU::GetInfoByProductIBlock(CATALOG_IBLOCK_ID);
                        if (is_array($arInfo)) {

                            $rsOffers = CIBlockElement::GetList(array(), array('IBLOCK_ID' => $arInfo['IBLOCK_ID'], 'PROPERTY_' . $arInfo['SKU_PROPERTY_ID'] => $product["ID"]));

                            $obElement = new CIBlockElement();

                            if ($arOffer = $rsOffers->Fetch()) {
                                $addToImportLog("Обновление");

                                // свойства торгвого предложения
                                $arOfferProps = array(
                                    $SKUPropertyId => $product["ID"],
                                    "ARTICULE" => $prod_id,
                                    "SIZE" => $size,
                                    "BARCODE" => $barcode,
                                    "SHIPDATE" => $shippingdate,
                                    "STOPPROMO" => $stop_promo,
                                    "COLOR" => $color
                                );
                                $arOfferFields = array(
                                    'NAME' => $offerName,
                                    'IBLOCK_ID' => OFFERS_IBLOCK_ID,
                                    'ACTIVE' => 'Y',
                                    'PROPERTY_VALUES' => $arOfferProps
                                );

                                $offer = $obElement->Update($arOffer["ID"], $arOfferFields);

                                if($offer){

                                    $addToImportLog($arOffer["ID"] . " - Обновлен");

                                    /***
                                     * Цена товара
                                     */

                                    //добавляет параметры товара
                                    $arCCatalogProductFields = array(
                                        "ID" => $arOffer["ID"],
                                        "VAT_ID" => 1, //тип НДС
                                        "VAT_INCLUDED" => "Y", //НДС входит в стоимость
                                        "WEIGHT" => $weight,
                                        "QUANTITY" => $quantity,
                                        "PURCHASING_PRICE" => $basewholeprice
                                    );

                                    if (CCatalogProduct::Update($arOffer["ID"], $arCCatalogProductFields)) {
                                        $addToImportLog("CCatalogProduct::Update " . $arOffer["ID"]);

                                        $PRICE_TYPE_ID = 1;

                                        $arPriceFields = Array(
                                            "PRODUCT_ID" => $arOffer["ID"],
                                            "CATALOG_GROUP_ID" => $PRICE_TYPE_ID,
                                            "PRICE" => $price,
                                            "CURRENCY" => $currency
                                        );

                                        $res = CPrice::GetList(
                                            array(),
                                            array(
                                                "PRODUCT_ID" => $arOffer["ID"],
                                                "CATALOG_GROUP_ID" => $PRICE_TYPE_ID
                                            )
                                        );

                                        if ($arr = $res->Fetch()) {
                                            CPrice::Update($arr["ID"], $arPriceFields);
                                            $addToImportLog("CPrice::Update " . $arOffer["ID"]);
                                        } else {
                                            CPrice::Add($arPriceFields);
                                            $addToImportLog("CPrice::Add " . $arOffer["ID"]);
                                        }


                                        /**
                                         * Остаток на складе
                                         */

                                        $rsStore = CCatalogStoreProduct::GetList(
                                            array(),
                                            array('PRODUCT_ID' => $arOffer["ID"], 'STORE_ID' => 1),
                                            false,
                                            false,
                                            array()
                                        );

                                        $arStoreFields = Array(
                                            "PRODUCT_ID" => $arOffer["ID"],
                                            "STORE_ID"   => 1,
                                            "AMOUNT"     => $quantity_storage,
                                        );

                                        if ($arStore = $rsStore->Fetch()){
                                            CCatalogStoreProduct::Update($arStore["ID"], $arStoreFields);
                                            $addToImportLog("CCatalogStoreProduct::Update " . $arStore['STORE_NAME']);
                                        }
                                        else {
                                            CCatalogStoreProduct::Add($arStoreFields);
                                            $addToImportLog("CCatalogStoreProduct::Add " . $arStore['STORE_NAME']);
                                        }

                                    }
                                }

                            }
                            else {
                                $addToImportLog("Добавление - " . $product["ID"]);

                                $obElement = new CIBlockElement();

                                // свойства торгвого предложения
                                $arOfferProps = array(
                                    $SKUPropertyId => $product["ID"],
                                    "ARTICULE" => $prod_id,
                                    "SIZE" => $size,
                                    "BARCODE" => $barcode,
                                    "SHIPDATE" => $shippingdate,
                                    "STOPPROMO" => $stop_promo,
                                    "COLOR" => $color
                                );
                                $arOfferFields = array(
                                    'EXTERNAL_ID' => $sku,
                                    'NAME' => $offerName,
                                    'IBLOCK_ID' => OFFERS_IBLOCK_ID,
                                    'ACTIVE' => 'Y',
                                    'PROPERTY_VALUES' => $arOfferProps
                                );

                                if ($offerId = $obElement->Add($arOfferFields)) {
                                    /***
                                     * Цена товара
                                     */

                                    //добавляет параметры товара
                                    $arCCatalogProductFields = array(
                                        "ID" => $offerId,
                                        "VAT_ID" => 1, //тип НДС
                                        "VAT_INCLUDED" => "Y", //НДС входит в стоимость
                                        "WEIGHT" => $weight,
                                        "QUANTITY" => $quantity
                                    );

                                    if (CCatalogProduct::Add($arCCatalogProductFields)) {
                                        $addToImportLog("Добавлены параметры товара к элементу каталога " . $offerId);

                                        $PRICE_TYPE_ID = 1;

                                        $arPriceFields = Array(
                                            "PRODUCT_ID" => $offerId,
                                            "CATALOG_GROUP_ID" => $PRICE_TYPE_ID,
                                            "PRICE" => $price,
                                            "CURRENCY" => $currency
                                        );

                                        $res = CPrice::GetList(
                                            array(),
                                            array(
                                                "PRODUCT_ID" => $offerId,
                                                "CATALOG_GROUP_ID" => $PRICE_TYPE_ID
                                            )
                                        );

                                        if ($arr = $res->Fetch()) {
                                            CPrice::Update($arr["ID"], $arPriceFields);
                                            $addToImportLog("Цена обновлена " . $offerId);
                                        } else {
                                            CPrice::Add($arPriceFields);
                                            $addToImportLog("Цена добавлена " . $offerId);
                                        }

                                        $arStoreFields = Array(
                                            "PRODUCT_ID" => $offerId,
                                            "STORE_ID"   => 1,
                                            "AMOUNT"     => $quantity_storage,
                                        );

                                        if(CCatalogStoreProduct::Add($arStoreFields))
                                            $addToImportLog("Добавлен остаток на складе");

                                    } else
                                        $addToImportLog("Ошибка добавления параметров цены");
                                }
                                else {
                                    $addToImportLog($obElement->LAST_ERROR);
                                }
                            }
                        }
                    }
                    $addToImportLog("---------- end OFFER ---------");

                }

                $startLine++;
                $iteration_inner++;

                if ($iteration_inner == PARTITION) {

                    $partitional = $startLine + PARTITION;

                    $addToImportLog("\n---------- ended   log ---------");
                    file_put_contents($logFile, $currentLog);

                    return "ImportOffersFullCatalogStep(" . $startLine . ", " . $partitional . ");";

                }
            } elseif ($tmp_startLine !== 0) {
                $iteration++;
            }
        }
        elseif($startLine == 0){
            $startLine++;
        }

    }
    $addToImportLog("\n---------- ended   log ---------");
    file_put_contents($logFile, $currentLog);
}

function ImportVendorsCatalog(){
    /**
     * Лог файл
     */

    $logFile = $_SERVER["DOCUMENT_ROOT"] . '/sync/log/' .date("y-m-d")."-import-import-vendors-log_".time().".txt";
    $currentLog = file_get_contents($logFile);

    $addToImportLog = function ($text) use (&$currentLog) {
        $currentLog .= $text . "\n";
    };

    $addToImportLog("\n---------- started log ---------\n");

    /**
     * скачиваем файл
     */

    $fileUrl = 'http://stripmag.ru/datafeed/bitrix_vendors.csv';
    $saveTo = $_SERVER["DOCUMENT_ROOT"] . "/sync/upload_csv/bitrix_vendors.csv";

    $bytes = curl_download($fileUrl, $saveTo);

    if(intval($bytes) > 0){
        $handle = fopen($saveTo, "r");

        if(($buffer = fgets($handle)) == false) {
            $addToImportLog("Ошибка. Файл пустой.\n");
        }
        else {
            $addToImportLog("OK. Файл успешно скачан.\n");

            $nextExec = ConvertTimeStamp(time()+10, "FULL");
            CAgent::AddAgent(
                "ImportVendorsCatalogStep(0, " . PARTITION .");", // имя функции
                "",                         // идентификатор модуля
                "N",                         // агент не критичен к кол-ву запусков
                1,                          // интервал запуска - 1 секунда
                $nextExec,                          // дата первой проверки на запуск
                "Y",                         // агент активен
                $nextExec,                          // дата первого запуска
                30);
        }
    }

    $addToImportLog("\n---------- ended   log ---------");
    file_put_contents($logFile, $currentLog);

    return "ImportVendorsCatalog();";
}
function ImportVendorsCatalogStep($startLine, $partitional){
    CModule::IncludeModule("iblock");
    $csvFile = $_SERVER["DOCUMENT_ROOT"] . "/sync/upload_csv/bitrix_vendors.csv";

    /**
     * Лог файл
     */

    $logFile = $_SERVER["DOCUMENT_ROOT"] . '/sync/log/' .date("y-m-d")."-import-import-vendors-step-" . $startLine ."-log_".time().".txt";
    $currentLog = file_get_contents($logFile);

    $addToImportLog = function ($text) use (&$currentLog) {
        $currentLog .= $text . "\n";
    };

    $addToImportLog("\n---------- started log ---------\n");

    $handle = fopen($csvFile, "r");

    $paramsTranslit = Array(
        "max_len" => "100", // обрезает символьный код до 100 символов
        "change_case" => "L", // буквы преобразуются к нижнему регистру
        "replace_space" => "_", // меняем пробелы на нижнее подчеркивание
        "replace_other" => "_", // меняем левые символы на нижнее подчеркивание
        "delete_repeat_replace" => "true", // удаляем повторяющиеся нижние подчеркивания
        "use_google" => "false", // отключаем использование google
    );

    $el = new CIBlockElement;

    $iteration = 0;
    $iteration_inner = 0;
    $tmp_startLine = $startLine;

    while(($buffer = fgets($handle)) !== false) {
        $row = explode(";", $buffer);

        if($startLine !== 0) {

            if (($tmp_startLine == 0) || ($iteration == $tmp_startLine)) {

                $vendor_id = $row[0];

                if(!empty($vendor_id)) {

                    $name = str_replace("\"", "", $row[1]);
                    $detail_text = str_replace("\"", "", $row[2]);
                    $country = str_replace("\"", "", $row[3]);
                    $detail_text_type = str_replace("\"", "", $row[4]);

                    $arProperties = Array();
                    $arProperties["COUNTRY"] = $country;

                    $rsElem = CIBlockElement::Getlist(
                        array(),
                        array(
                            "EXTERNAL_ID" => $vendor_id,
                            "IBLOCK_ID" => VENDORS_IBLOCK_ID,
                        )
                    );

                    if ($elem = $rsElem->GetNext()) {

                        $arLoadProductArray = Array(
                            "EXTERNAL_ID" => $vendor_id,
                            "XML_ID" => $vendor_id,
                            "IBLOCK_SECTION_ID" => false,
                            "IBLOCK_ID" => VENDORS_IBLOCK_ID,
                            "CODE" => CUtil::translit($name, "ru", $paramsTranslit),
                            "NAME" => $name,
                            "ACTIVE" => "Y",
                            "DETAIL_TEXT" => $detail_text,
                            "DETAIL_TEXT_TYPE" => $detail_text_type
                        );

                        $isUpdate = $el->Update($elem["ID"], $arLoadProductArray);

                        if($isUpdate){
                            $addToImportLog("Изменен ID: " . $elem["ID"] . "\t vendor_id: " . $vendor_id . "\n");
                            CIBlockElement::SetPropertyValuesEx($elem["ID"], false, $arProperties);
                        }
                        else {
                            $addToImportLog($el->LAST_ERROR);
                        }

                    } else {

                        $arLoadProductArray = Array(
                            "EXTERNAL_ID" => $vendor_id,
                            "XML_ID" => $vendor_id,
                            "IBLOCK_SECTION_ID" => false,
                            "IBLOCK_ID" => VENDORS_IBLOCK_ID,
                            "CODE" => CUtil::translit($name, "ru", $paramsTranslit),
                            "PROPERTY_VALUES" => $arProperties,
                            "NAME" => $name,
                            "ACTIVE" => "Y",
                            "DETAIL_TEXT" => $detail_text,
                            "DETAIL_TEXT_TYPE" => $detail_text_type
                        );

                        if ($VENDOR = $el->Add($arLoadProductArray)) {
                            $addToImportLog("Новый ID: " . $VENDOR . "\t vendor_id: " . $vendor_id . "\n");
                        }
                        else {
                            $addToImportLog($el->LAST_ERROR);
                        }
                    }
                }

                $startLine++;
                $iteration_inner++;

                if ($iteration_inner == PARTITION) {

                    $partitional = $startLine + PARTITION;

                    $addToImportLog("\n---------- ended   log ---------");
                    file_put_contents($logFile, $currentLog);

                    return "ImportVendorsCatalogStep(" . $startLine . ", " . $partitional . ");";

                }
            } elseif ($tmp_startLine !== 0) {
                $iteration++;
            }
        }
        elseif($startLine == 0){
            $startLine++;
        }

    }

    $addToImportLog("\n---------- ended   log ---------");
    file_put_contents($logFile, $currentLog);
}

function ImportColorsCatalog(){
    /**
     * Лог файл
     */

    $logFile = $_SERVER["DOCUMENT_ROOT"] . '/sync/log/' .date("y-m-d")."-import-import-colors-log_".time().".txt";
    $currentLog = file_get_contents($logFile);

    $addToImportLog = function ($text) use (&$currentLog) {
        $currentLog .= $text . "\n";
    };

    $addToImportLog("\n---------- started log ---------\n");

    /**
     * скачиваем файл
     */

    $fileUrl = 'http://stripmag.ru/datafeed/bitrix_colors.csv';
    $saveTo = $_SERVER["DOCUMENT_ROOT"] . "/sync/upload_csv/bitrix_colors.csv";

    $bytes = curl_download($fileUrl, $saveTo);

    if(intval($bytes) > 0){
        $handle = fopen($saveTo, "r");

        if(($buffer = fgets($handle)) == false) {
            $addToImportLog("Ошибка. Файл пустой.\n");
        }
        else {
            $addToImportLog("OK. Файл успешно скачан.\n");

            $nextExec = ConvertTimeStamp(time()+10, "FULL");
            CAgent::AddAgent(
                "ImportColorsCatalogStep(0, " . PARTITION .");", // имя функции
                "",                         // идентификатор модуля
                "N",                         // агент не критичен к кол-ву запусков
                1,                          // интервал запуска - 1 секунда
                $nextExec,                          // дата первой проверки на запуск
                "Y",                         // агент активен
                $nextExec,                          // дата первого запуска
                30);
        }
    }

    $addToImportLog("\n---------- ended   log ---------");
    file_put_contents($logFile, $currentLog);

    return "ImportColorsCatalog();";
}
function ImportColorsCatalogStep($startLine, $partitional){
    $csvFile = $_SERVER["DOCUMENT_ROOT"] . "/sync/upload_csv/bitrix_colors.csv";

    if (CModule::IncludeModule('highloadblock')) {
        $arHLBlock = Bitrix\Highloadblock\HighloadBlockTable::getById(COLORS_HIGHILOADBLOCK_ID)->fetch();
        $obEntity = Bitrix\Highloadblock\HighloadBlockTable::compileEntity($arHLBlock);
        $strEntityDataClass = $obEntity->getDataClass();
    }

    /**
     * Лог файл
     */

    $logFile = $_SERVER["DOCUMENT_ROOT"] . '/sync/log/' .date("y-m-d")."-import-import-colors-step-" . $startLine ."-log_".time().".txt";
    $currentLog = file_get_contents($logFile);

    $addToImportLog = function ($text) use (&$currentLog) {
        $currentLog .= $text . "\n";
    };

    $addToImportLog("\n---------- started log ---------\n");

    $handle = fopen($csvFile, "r");

    $iteration = 0;
    $iteration_inner = 0;
    $tmp_startLine = $startLine;

    while(($buffer = fgets($handle)) !== false) {
        $row = explode(";", $buffer);

        if($startLine !== 0) {

            if (($tmp_startLine == 0) || ($iteration == $tmp_startLine)) {

                $color = str_replace("\"", "", $row[0]);
                $color_code = str_replace("\"", "", $row[1]);

                if (empty($color))
                    continue;

                $rsData = $strEntityDataClass::getList(array(
                    "select" => array("*"),
                    "order" => array("ID" => "ASC"),
                    "filter" => array("UF_XML_ID" => $color_code)
                ));

                if ($arData = $rsData->Fetch()) {
                    $arElementFields = array(
                        'UF_NAME' => $color,
                        'UF_XML_ID' => $color_code
                    );
                    $obResult = $strEntityDataClass::update($arData["ID"], $arElementFields);
                    $ID = $obResult->getID();
                    $bSuccess = $ID > 0;

                    if ($bSuccess)
                        $addToImportLog("Цвет обновлен: " . $color . "ID = " . $arData["ID"]);
                    else
                        $addToImportLog("Ошибка обнолвения: " . $color . "ID = " . $arData["ID"]);
                } else {
                    $arElementFields = array(
                        'UF_NAME' => $color,
                        'UF_XML_ID' => $color_code
                    );
                    $obResult = $strEntityDataClass::add($arElementFields);
                    $ID = $obResult->getID();
                    $bSuccess = $ID > 0;

                    if ($bSuccess)
                        $addToImportLog("Цвет добавлен: " . $color . "ID = " . $arData["ID"]);
                    else
                        $addToImportLog("Ошибка добавления: " . $color . "ID = " . $arData["ID"]);
                }

                $startLine++;
                $iteration_inner++;

                if ($iteration_inner == PARTITION) {

                    $partitional = $startLine + PARTITION;

                    $addToImportLog("---------- ended   log ---------");
                    file_put_contents($logFile, $currentLog);

                    return "ImportColorsCatalogStep(" . $startLine . ", " . $partitional . ");";

                }
            } elseif ($tmp_startLine !== 0) {
                $iteration++;
            }
        }
        elseif($startLine == 0){
            $startLine++;
        }

    }
    $addToImportLog("---------- ended   log ---------");
    file_put_contents($logFile, $currentLog);
}

function curl_download($url, $saveTo){
    $ch = curl_init ($url);
    curl_setopt($ch, CURLOPT_HEADER, 0);
    curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
    curl_setopt($ch, CURLOPT_BINARYTRANSFER,1);

    $raw = curl_exec($ch);
    curl_close ($ch);

    if ( base64_decode(base64_encode($raw)) === $raw){

        if(file_exists($saveTo)){
            unlink($saveTo);
        }
        $fp = fopen($saveTo,'x');

        $count_bytes = fwrite($fp, $raw);
        fclose($fp);

        return $count_bytes;

    } else {
        return false;
    }
}
function makeArraySection($first, $second, $third){
    if((strlen($first) > 0) && (strlen($second) > 0) && (strlen($third) > 0)){
        return array($first, $second, $third);
    }

    if((strlen($first) > 0) && (strlen($second) > 0) && (strlen($third) <= 0)){
        return array($first, $second);
    }

    if((strlen($first) > 0) && (strlen($second) <= 0) && (strlen($third) <= 0)){
        return array($first);
    }
}

?>
