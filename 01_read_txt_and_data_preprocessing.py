import re
import time
import datetime
import collections
import pandas as pd
import warnings
warnings.filterwarnings("ignore")

# 관리자 행동 및 참여자 인입 기본 패턴
ACTIONS = ["채팅방 관리자가 메시지를 가렸습니다.", 
           "님을 내보냈습니다.",
           "님이 나갔습니다.",
           "님이 들어왔습니다."]

# 카카오톡 채팅 데이터 읽기
def read_kakao_txt_file(input_file_name, has_header=True):
    date_sep = '일 ---------------'
    context_list, tmp_contenxt_list = [], []
    with open(input_file_name, "r", encoding="utf-8-sig") as input_file:
        index = 0
        for line in input_file:
            line = line.strip()
            start_index = 0
            if has_header:
                start_index = 4
            if index == start_index:
                tmp_contenxt_list.append(line)
            else:
                if line.endswith(date_sep):
                    context_list.append(tmp_contenxt_list)
                    tmp_contenxt_list = []
                tmp_contenxt_list.append(line)
            index += 1
    print("주어진 텍스트 파일 {}의 일자별 context는 {}건입니다.".format(input_file_name, len(context_list)))
    return context_list

# 작성자+작성시간별 메시지 합치기
def split_talk_by_user(context):
    whole_txt, merge_txt = [], []
    for index, element in enumerate(context):
        start_str = re.match(r'\[.*?\]\s\[[오전|오후].*?\]', element)
        if start_str:
            whole_txt.append(' '.join(merge_txt))
            merge_txt = []
            merge_txt.append(element)
            if index == len(context)-1:
                whole_txt.append(' '.join(merge_txt))
        else:
            is_contain = False
            for action in ACTIONS:
                if action in element:
                    is_contain = True
            if is_contain:
                if merge_txt:
                    whole_txt.append(' '.join(merge_txt))
                whole_txt.append(element)
                merge_txt = []
            else:
                merge_txt.append(element)
    return [e for e in whole_txt if e.strip()]


# 기준일 추출
def get_date(line):
    date_sep = '---------------'
    full_date = line.replace(date_sep, '').strip()
    talk_date, day_name = full_date[:-4], full_date[-3:]
    talk_date = datetime.datetime.strptime(talk_date, '%Y년 %m월 %d일')
    talk_date = datetime.datetime.strftime(talk_date, '%Y-%m-%d')
    return talk_date, day_name


# 작성자, 작성 시간, 메시지 추출
def get_writer_and_wrote_at_and_msg(line):
    def convert_time(ko_wrote_at):
        import time

        if '오전' in ko_wrote_at:
            time_str = ko_wrote_at.replace('오전', 'am')
        elif '오후' in ko_wrote_at:
            time_str = ko_wrote_at.replace('오후', 'pm')

        time_str = datetime.datetime.strptime(time_str, '%p %I:%M')
        time_str = datetime.datetime.strftime(time_str, '%H:%M')
        return time_str
    
    split_line = re.split('\]|\[', line)
    split_line = [e for e in split_line if e.strip()]
    writer, wrote_at = split_line[0], convert_time(split_line[1])
    msg = line.split(split_line[1]+']')[1].strip()
    msg = msg if msg else None
    return writer, wrote_at, msg


# 관리자, 참여자 행동 확인
def get_actions(line):
    global action_msg
    
    writer = '관리자'
    if ACTIONS[0] in line:
        action_msg = '메시지 가리기'
    elif ACTIONS[1] in line:
        action_msg = '내보내기'
    elif ACTIONS[2] in line:
        writer, action_msg = line.replace(ACTIONS[3], ''), '나가기'
    elif ACTIONS[3] in line:
        writer, action_msg = line.replace(ACTIONS[4], ''), '들어오기'
    return writer, action_msg

# 공지 여부 확인
def check_notice(text):
    start_str = re.match(r"톡게시판 '공지': ", text)
    is_notice_action = False
    if start_str:
        is_notice_action = True
    return is_notice_action
    

# 위 함수를 사용해 만든 main()
def main(input_file_name, has_header, save_file_name, sep='\t'):
    try:
        # read txt
        context_list = read_kakao_txt_file(input_file_name, has_header)
        # data processing (row data to semi-structured data)
        cols =  'talk_date day_name writer wrote_at msg action_msg is_talking_activity is_notice_action'
        Msg_info = collections.namedtuple("Msg_info", cols)
        all_rows = []
        for context in context_list:
            talk_date, day_name = get_date(context[0])
            contents_list = split_talk_by_user(context[1:])
            for index, content in enumerate(contents_list):
                content = content.strip()
                start_regex = r'\[.*?\]\s\[[오전|오후].*?\]'
                start_str = re.match(start_regex, content)
                is_talking_activity, is_notice_action = True, False
                wrote_at, action_msg = None, None
                if start_str:
                    writer, wrote_at, msg = get_writer_and_wrote_at_and_msg(content)
                    is_notice_action = check_notice(msg)
                else:
                    writer, action_msg = get_actions(content)
                    msg,is_talking_activity = content, False
                
                msg = re.sub(r'\s+', ' ', msg).strip()
                msg_obj = Msg_info._make([talk_date, day_name, writer, wrote_at, msg,
                                          action_msg, is_talking_activity, is_notice_action])
                all_rows.append(msg_obj._asdict())
                del msg_obj
                
        # a dictonary list to a dataframe on pandas
        df = pd.DataFrame.from_dict(all_rows)
        df['is_deleted_msg'] = df['msg'].apply(lambda x: True if x.strip() == '삭제된 메시지입니다.' else False)
        df['is_emoji'] = df['msg'].apply(lambda x: True if x.strip() == '이모티콘' else False) 
        df['is_picture'] = df['msg'].apply(lambda x: True if x.strip() == '사진' else False)
        df['is_deleted_msg'] = df['msg'].apply(lambda x: True if x.strip() == '삭제된 메시지입니다.' else False)
        df['is_search'] = df['msg'].apply(lambda x: True if x.startswith('샵검색: #') else False)

        # save as csv format (default is tsv) 
        df.to_csv(save_file_name, sep=sep, index=False)
            
    except Exception as e:
#         print(context)
        print(e)
		
if __name__ == '__main__':
	input_file_name = 'input_your_kakaotalk_chat_file.txt'
	save_file_name = 'outpur_yours.csv'
	main(file_tag, input_file_name, save_file_name)
