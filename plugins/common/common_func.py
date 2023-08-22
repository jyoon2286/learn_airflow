def get_sftp():
    print("stfp work in-progress...")

def register(name, sex, *args):
    print(f'Name: {name}')
    print(f'SeX: {sex}')
    print(f'other information: {args}')

def register2(name, sex, *args, **kwargs):
    print(f'Name: {name}')
    print(f'SeX: {sex}')
    print(f'other information: {args}')
    email = kwargs['email'] or ''
    phone = kwargs['phone'] or ''
    if email:
        print(email)   
    if phone:
        print(phone)

